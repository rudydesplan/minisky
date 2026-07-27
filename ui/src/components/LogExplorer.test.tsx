import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, describe, expect, it, vi } from 'vitest';
import LogExplorer from './LogExplorer';

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('LogExplorer failures', () => {
  it('renders a logging read failure instead of an empty state', async () => {
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL) => {
      if (String(input).endsWith('/entries')) return new Response('', { status: 503 });
      return new Response('[]', { status: 200 });
    }));

    render(<LogExplorer />);
    expect(await screen.findByRole('alert')).toHaveTextContent(
      'Unable to load centralized logs. Verify that logging is available.',
    );
    expect(screen.queryByText('No log entries yet.')).not.toBeInTheDocument();
  });

  it('rejects malformed successful logging responses', async () => {
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL) => {
      if (String(input).endsWith('/entries')) {
        return new Response(JSON.stringify({ entries: [{ insertId: 'missing-fields' }] }), { status: 200 });
      }
      return new Response('[]', { status: 200 });
    }));

    render(<LogExplorer />);
    expect(await screen.findByRole('alert')).toHaveTextContent(
      'Centralized logs returned an invalid entry.',
    );
  });

  it.each([
    { textPayload: 42 },
    { jsonPayload: [] },
    { resource: { type: 'global', labels: { name: 42 } } },
  ])('rejects malformed optional log fields: %o', async optional => {
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL) => {
      if (String(input).endsWith('/entries')) {
        return new Response(JSON.stringify({ entries: [{
          insertId: 'entry-1',
          timestamp: '2026-07-26T10:00:00Z',
          severity: 'INFO',
          logName: 'projects/p/logs/test',
          ...optional,
        }] }), { status: 200 });
      }
      return new Response('[]', { status: 200 });
    }));
    render(<LogExplorer />);
    expect(await screen.findByRole('alert')).toHaveTextContent(
      'Centralized logs returned an invalid entry.',
    );
  });

  it('discards a superseded container-log response', async () => {
    let resolveFirst: ((response: Response) => void) | undefined;
    const fetchMock = vi.fn((input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith('/entries')) {
        return Promise.resolve(new Response(JSON.stringify({ entries: [] }), { status: 200 }));
      }
      if (url.endsWith('/container')) {
        return Promise.resolve(new Response(JSON.stringify([
          { name: 'minisky-serverless-first', status: 'Up', image: 'first' },
          { name: 'minisky-serverless-second', status: 'Up', image: 'second' },
        ]), { status: 200 }));
      }
      if (url.includes('first')) {
        return new Promise<Response>(resolve => { resolveFirst = resolve; });
      }
      return Promise.resolve(new Response('new output', { status: 200 }));
    });
    vi.stubGlobal('fetch', fetchMock);
    const user = userEvent.setup();
    render(<LogExplorer />);

    await user.click(screen.getByRole('button', { name: 'Container Output' }));
    const selector = await screen.findByRole('combobox', { name: 'Select Container' });
    await user.click(selector);
    await user.click(screen.getByRole('option', { name: /first/i }));
    await user.click(selector);
    await user.click(screen.getByRole('option', { name: /second/i }));
    expect(await screen.findByText('new output')).toBeInTheDocument();
    resolveFirst?.(new Response('old output', { status: 200 }));
    expect(screen.queryByText('old output')).not.toBeInTheDocument();
  });

  it('never attributes previous output to a newly selected container', async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith('/entries')) {
        return new Response(JSON.stringify({ entries: [] }), { status: 200 });
      }
      if (url.endsWith('/container')) {
        return new Response(JSON.stringify([
          { name: 'minisky-serverless-first', status: 'Up', image: 'first' },
          { name: 'minisky-serverless-second', status: 'Up', image: 'second' },
        ]), { status: 200 });
      }
      if (url.includes('first')) return new Response('first output', { status: 200 });
      return new Response('', { status: 503 });
    });
    vi.stubGlobal('fetch', fetchMock);
    const user = userEvent.setup();
    render(<LogExplorer />);

    await user.click(screen.getByRole('button', { name: 'Container Output' }));
    const selector = await screen.findByRole('combobox', { name: 'Select Container' });
    await user.click(selector);
    await user.click(screen.getByRole('option', { name: /first/i }));
    expect(await screen.findByText('first output')).toBeInTheDocument();

    await user.click(selector);
    await user.click(screen.getByRole('option', { name: /second/i }));
    expect(await screen.findByRole('alert')).toHaveTextContent('Unable to load container output');
    expect(screen.queryByText('first output')).not.toBeInTheDocument();
    expect(screen.getByRole('alert')).not.toHaveTextContent('Showing stale data');
  });
});
