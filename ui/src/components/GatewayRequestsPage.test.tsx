import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, describe, expect, it, vi } from 'vitest';
import GatewayRequestsPage from './GatewayRequestsPage';

const record = {
  timestamp: '2026-07-26T10:00:00Z',
  requestId: 'request-1',
  traceId: '4bf92f3577b34da6a3ce929d0e0e4736',
  method: 'GET',
  route: '/v1/projects/{id}/instances/{id}',
  service: 'compute.googleapis.com',
  status: 200,
  latencyMs: 3.5,
  replayable: false,
};

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('GatewayRequestsPage', () => {
  it('renders named filters, semantic headers, and a visible trace ID', async () => {
    vi.stubGlobal('fetch', vi.fn(async () =>
      new Response(JSON.stringify({ requests: [record] }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      }),
    ));

    render(<GatewayRequestsPage />);

    expect(await screen.findByRole('cell', { name: record.traceId })).toBeInTheDocument();
    expect(screen.getByRole('combobox', { name: 'Service' })).toBeInTheDocument();
    expect(screen.getByRole('combobox', { name: 'Method' })).toBeInTheDocument();
    for (const header of ['Method', 'Route and service', 'Trace ID', 'Status', 'Latency', 'Actions']) {
      expect(screen.getByRole('columnheader', { name: header })).toBeInTheDocument();
    }
    expect(screen.getByRole('button', { name: 'Replay GET request-1' })).toBeDisabled();
  });

  it('shows malformed responses as an explicit error', async () => {
    vi.stubGlobal('fetch', vi.fn(async () =>
      new Response(JSON.stringify({ requests: [{ requestId: 'broken' }] }), { status: 200 }),
    ));

    render(<GatewayRequestsPage />);
    expect(await screen.findByRole('alert')).toHaveTextContent('Malformed request record');
  });

  it.each([
    { ...record, traceId: 42 },
    { ...record, status: null },
    { ...record, latencyMs: null },
  ])('rejects malformed consumed optional fields: %o', async malformed => {
    vi.stubGlobal('fetch', vi.fn(async () =>
      new Response(JSON.stringify({ requests: [malformed] }), { status: 200 }),
    ));
    render(<GatewayRequestsPage />);
    expect(await screen.findByRole('alert')).toHaveTextContent('Malformed request record');
    expect(screen.queryByText('No matching requests.')).not.toBeInTheDocument();
  });

  it('marks retained records stale after a refresh failure', async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ requests: [record] }), { status: 200 }))
      .mockResolvedValueOnce(new Response('', { status: 503 }));
    vi.stubGlobal('fetch', fetchMock);
    const user = userEvent.setup();
    render(<GatewayRequestsPage />);
    await screen.findByText(record.route);
    await user.click(screen.getByRole('button', { name: 'Refresh' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('Showing stale gateway records');
    expect(screen.getByText(record.route)).toBeInTheDocument();
  });

  it('filters by visible trace ID', async () => {
    vi.stubGlobal('fetch', vi.fn(async () =>
      new Response(JSON.stringify({ requests: [record] }), { status: 200 }),
    ));
    const user = userEvent.setup();
    render(<GatewayRequestsPage />);

    await screen.findByText(record.route);
    await user.type(screen.getByRole('textbox', { name: 'Route, request ID, or trace ID' }), 'missing');
    await waitFor(() => expect(screen.getByText('No matching requests.')).toBeInTheDocument());
  });
});
