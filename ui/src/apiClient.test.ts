import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  checkedMutation,
  installDashboardFetch,
  requireOk,
  responseErrorMessage,
  safeRequestError,
} from './apiClient';

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('responseErrorMessage', () => {
  it('provides shared actionable authorization errors', async () => {
    expect(await responseErrorMessage(new Response('', { status: 401 }), 'fallback'))
      .toContain('dashboard token');
    expect(await responseErrorMessage(new Response('', { status: 403 }), 'fallback'))
      .toContain('Permission denied');
  });

  it('never renders backend body secrets, URLs, or paths', async () => {
    const response = new Response(JSON.stringify({
      error: {
        message: 'password=hunter2 client_secret=secret api_key=key https://internal.test/private /var/lib/minisky/state',
      },
    }), { status: 400, headers: { 'content-type': 'application/json' } });

    const message = await responseErrorMessage(response, 'fallback');
    expect(message).toBe('fallback');
    expect(message).not.toMatch(/hunter2|client_secret|api_key|https:|\/var\//);
  });

  it('preserves only trusted request errors across catch boundaries', async () => {
    let safeError: unknown;
    try {
      await requireOk(new Response('client_secret=hidden', { status: 503 }), 'The local service is unavailable. Start it and retry.');
    } catch (error) {
      safeError = error;
    }

    expect(safeRequestError(safeError, 'Safe fallback.')).toBe('The local service is unavailable. Start it and retry.');
    expect(safeRequestError(new Error('https://private/path?api_key=secret'), 'Safe fallback.'))
      .toBe('Safe fallback.');
  });

  it.each([
    ['GKE', 'GKE cluster provisioning failed. Check kind availability and the cluster name.'],
    ['Serverless', 'Serverless resource deletion failed. Remove dependent triggers and retry.'],
    ['App Engine', 'App Engine version deletion failed. Stop traffic to the version and retry.'],
    ['VPC', 'VPC deletion failed. Remove attached subnetworks and instances first.'],
  ])('preserves trusted static %s operation guidance without reading bodies', async (_operation, guidance) => {
    let error: unknown;
    try {
      await requireOk(new Response(
        'password=hidden client_secret=hidden api_key=hidden https://private/path',
        { status: 500 },
      ), guidance);
    } catch (cause) {
      error = cause;
    }

    expect(safeRequestError(error, 'Network fallback.')).toBe(guidance);
    expect(safeRequestError(error, 'Network fallback.')).not.toMatch(/password|client_secret|api_key|https:/);
  });

  it('blocks mutation success handling on non-2xx without reading the body', async () => {
    const response = new Response('client_secret=hidden', { status: 500 });
    const text = vi.spyOn(response, 'text');
    vi.stubGlobal('fetch', vi.fn(async () => response));
    let refreshed = false;
    let error: unknown;

    try {
      await checkedMutation('/api/resource', { method: 'DELETE' }, 'Delete failed. Detach dependencies and retry.');
      refreshed = true;
    } catch (cause) {
      error = cause;
    }

    expect(refreshed).toBe(false);
    expect(safeRequestError(error, 'Network fallback.')).toBe('Delete failed. Detach dependencies and retry.');
    expect(text).not.toHaveBeenCalled();
  });

  it('uses the active project for diagnostics authorization and selection', async () => {
    const local = new Map([['minisky-active-project', 'project-alpha']]);
    vi.stubGlobal('localStorage', {
      getItem: (key: string) => local.get(key) ?? null,
      setItem: (key: string, value: string) => local.set(key, value),
      removeItem: (key: string) => local.delete(key),
      clear: () => local.clear(),
    });
    vi.stubGlobal('sessionStorage', {
      getItem: () => null,
      setItem: () => undefined,
      removeItem: () => undefined,
      clear: () => undefined,
    });
    const nativeFetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      void input;
      void init;
      return new Response('{}', { status: 200 });
    });
    vi.stubGlobal('fetch', nativeFetch);
    installDashboardFetch();

    await window.fetch('/api/diagnostics/requests');

    const [input, init] = nativeFetch.mock.calls[0];
    const url = new URL(input.toString(), window.location.href);
    expect(url.searchParams.get('project')).toBe('project-alpha');
    expect(new Headers(init?.headers).get('X-MiniSky-Project')).toBe('project-alpha');
  });
});
