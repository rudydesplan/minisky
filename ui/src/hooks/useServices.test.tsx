import { act, renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { useServices } from './useServices';

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe('useServices polling', () => {
  it('keeps polling single-flight and aborts the active request on unmount', async () => {
    vi.useFakeTimers();
    let signal: AbortSignal | undefined;
    const fetchMock = vi.fn((_input: RequestInfo | URL, init?: RequestInit) => {
      signal = init?.signal ?? undefined;
      return new Promise<Response>(() => undefined);
    });
    vi.stubGlobal('fetch', fetchMock);

    const { unmount } = renderHook(() => useServices());
    await act(async () => Promise.resolve());
    expect(fetchMock).toHaveBeenCalledTimes(1);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(12_000);
    });
    expect(fetchMock).toHaveBeenCalledTimes(1);

    unmount();
    expect(signal?.aborted).toBe(true);
  });

  it('reports an invalid services response instead of treating it as empty', async () => {
    vi.stubGlobal('fetch', vi.fn(async () =>
      new Response(JSON.stringify([{ id: 'broken' }]), { status: 200 }),
    ));
    const { result, unmount } = renderHook(() => useServices());

    await waitFor(() => {
      expect(result.current.error).toBe('Service status returned an invalid response.');
    });
    unmount();
  });

  it.each([
    { missingDeps: 'docker' },
    { backend: { profile: 'default', backend: 'docker', enabled: 'yes', source: 'config' } },
  ])('rejects malformed optional service fields: %o', async optional => {
    vi.stubGlobal('fetch', vi.fn(async () =>
      new Response(JSON.stringify([{
        id: 'storage',
        name: 'storage',
        label: 'Storage',
        status: 'RUNNING',
        port: null,
        description: 'Storage service',
        ...optional,
      }]), { status: 200 }),
    ));
    const { result, unmount } = renderHook(() => useServices());
    await waitFor(() => {
      expect(result.current.error).toBe('Service status returned an invalid response.');
    });
    unmount();
  });

  it('aborts an in-flight poll before a lifecycle action refresh', async () => {
    let pollSignal: AbortSignal | undefined;
    let resolveMutation: ((response: Response) => void) | undefined;
    const fetchMock = vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      if (String(input).includes('/start')) {
        return new Promise<Response>(resolve => { resolveMutation = resolve; });
      }
      pollSignal ??= init?.signal ?? undefined;
      return new Promise<Response>(() => undefined);
    });
    vi.stubGlobal('fetch', fetchMock);
    const { result, unmount } = renderHook(() => useServices());
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));

    act(() => {
      void result.current.handleStartContainer('storage');
    });
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    expect(pollSignal?.aborted).toBe(true);
    resolveMutation?.(new Response('', { status: 500 }));
    unmount();
  });

  it('serializes lifecycle mutations without aborting their POST requests', async () => {
    let startSignal: AbortSignal | undefined;
    let resolveStart: ((response: Response) => void) | undefined;
    const fetchMock = vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.includes('/start')) {
        startSignal = init?.signal ?? undefined;
        return new Promise<Response>(resolve => { resolveStart = resolve; });
      }
      if (url.includes('/stop')) {
        return Promise.resolve(new Response(null, { status: 204 }));
      }
      return new Promise<Response>(() => undefined);
    });
    vi.stubGlobal('fetch', fetchMock);
    const { result, unmount } = renderHook(() => useServices());
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));

    act(() => {
      void result.current.handleStartContainer('storage');
      void result.current.handleStopContainer('storage');
    });
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    expect(fetchMock.mock.calls.some(([input]) => String(input).includes('/stop'))).toBe(false);
    expect(startSignal).toBeUndefined();

    resolveStart?.(new Response(null, { status: 204 }));
    await waitFor(() => {
      expect(fetchMock.mock.calls.some(([input]) => String(input).includes('/stop'))).toBe(true);
    });
    expect(startSignal?.aborted).not.toBe(true);
    unmount();
  });
});
