import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  MenuItem,
  Select,
  TextField,
  Typography,
} from '@mui/material';
import RefreshIcon from '@mui/icons-material/Refresh';
import ReplayIcon from '@mui/icons-material/Replay';
import { requireOk, safeRequestError } from '../apiClient';

type GatewayRequest = {
  timestamp: string;
  requestId: string;
  traceId?: string;
  method: string;
  route: string;
  service: string;
  status: number;
  latencyMs: number;
  replayable: boolean;
};

function isGatewayRequest(value: unknown): value is GatewayRequest {
  if (typeof value !== 'object' || value === null) return false;
  const record = value as Record<string, unknown>;
  return typeof record.timestamp === 'string'
    && typeof record.requestId === 'string'
    && typeof record.method === 'string'
    && typeof record.route === 'string'
    && typeof record.service === 'string'
    && typeof record.status === 'number'
    && typeof record.latencyMs === 'number'
    && typeof record.replayable === 'boolean';
}

export default function GatewayRequestsPage() {
  const [requests, setRequests] = useState<GatewayRequest[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [service, setService] = useState('ALL');
  const [method, setMethod] = useState('ALL');
  const [search, setSearch] = useState('');
  const [replaying, setReplaying] = useState('');

  const load = useCallback(async (signal?: AbortSignal) => {
    setLoading(true);
    try {
      const response = await fetch('/api/diagnostics/requests', { signal });
      await requireOk(response, 'Gateway request loading failed. Refresh diagnostics and retry.');
      const data: unknown = await response.json();
      if (typeof data !== 'object' || data === null || !Array.isArray((data as { requests?: unknown }).requests)) {
        throw new Error('Malformed diagnostics response');
      }
      const parsed = (data as { requests: unknown[] }).requests;
      if (!parsed.every(isGatewayRequest)) throw new Error('Malformed request record');
      setRequests(parsed);
      setError('');
    } catch (err) {
      if (err instanceof DOMException && err.name === 'AbortError') return;
      setError(err instanceof Error ? err.message : 'Unable to load gateway requests');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    load(controller.signal);
    return () => controller.abort();
  }, [load]);

  const services = useMemo(
    () => ['ALL', ...Array.from(new Set(requests.map(request => request.service))).sort()],
    [requests],
  );
  const filtered = useMemo(() => requests.filter(request => {
    if (service !== 'ALL' && request.service !== service) return false;
    if (method !== 'ALL' && request.method !== method) return false;
    const needle = search.toLowerCase();
    return !needle
      || request.route.toLowerCase().includes(needle)
      || request.requestId.toLowerCase().includes(needle)
      || (request.traceId ?? '').toLowerCase().includes(needle);
  }), [method, requests, search, service]);

  const replay = async (request: GatewayRequest) => {
    if (!window.confirm(`Replay ${request.method} ${request.route} through the local gateway?`)) return;
    setReplaying(request.requestId);
    try {
      const response = await fetch(
        `/api/diagnostics/requests/${encodeURIComponent(request.requestId)}/replay`,
        { method: 'POST' },
      );
      await requireOk(response, 'Gateway request replay failed. Refresh diagnostics and retry.');
      await load();
    } catch (err) {
      setError(safeRequestError(err, 'Unable to connect while replaying the gateway request.'));
    } finally {
      setReplaying('');
    }
  };

  return (
    <Box sx={{ p: 4, minHeight: '100%', background: '#f8f9fa' }}>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mb: 3, flexWrap: 'wrap' }}>
        <Box>
          <Typography variant="h5" sx={{ fontWeight: 600 }}>Gateway Requests</Typography>
          <Typography variant="body2" color="text.secondary">
            Bounded local access records. Request bodies and credentials are never shown.
          </Typography>
        </Box>
        <Box sx={{ flex: 1 }} />
        <Button startIcon={<RefreshIcon />} onClick={() => load()} disabled={loading}>Refresh</Button>
      </Box>

      {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}

      <Box sx={{ display: 'flex', gap: 2, mb: 2, flexWrap: 'wrap' }}>
        <TextField
          size="small"
          label="Route, request ID, or trace ID"
          value={search}
          onChange={event => setSearch(event.target.value)}
          sx={{ minWidth: 280 }}
        />
        <Select size="small" value={service} onChange={event => setService(event.target.value)}>
          {services.map(value => <MenuItem key={value} value={value}>{value === 'ALL' ? 'All services' : value}</MenuItem>)}
        </Select>
        <Select size="small" value={method} onChange={event => setMethod(event.target.value)}>
          {['ALL', 'GET', 'POST', 'PUT', 'PATCH', 'DELETE'].map(value => (
            <MenuItem key={value} value={value}>{value === 'ALL' ? 'All methods' : value}</MenuItem>
          ))}
        </Select>
        <Chip label={`${filtered.length} requests`} />
      </Box>

      {loading && requests.length === 0 ? (
        <Box sx={{ display: 'grid', placeItems: 'center', py: 10 }}><CircularProgress /></Box>
      ) : filtered.length === 0 ? (
        <Typography color="text.secondary" sx={{ py: 8, textAlign: 'center' }}>No matching requests.</Typography>
      ) : (
        <Box sx={{ border: '1px solid #dadce0', borderRadius: 2, overflow: 'auto', background: '#fff' }}>
          {filtered.map(request => (
            <Box
              key={request.requestId}
              sx={{
                display: 'grid',
                gridTemplateColumns: '90px minmax(280px, 1fr) 90px 100px 130px',
                gap: 2,
                alignItems: 'center',
                px: 2,
                py: 1.5,
                borderBottom: '1px solid #eee',
                minWidth: 850,
              }}
            >
              <Chip label={request.method} size="small" />
              <Box sx={{ minWidth: 0 }}>
                <Typography sx={{ fontFamily: 'monospace', fontSize: 13 }} noWrap>{request.route}</Typography>
                <Typography variant="caption" color="text.secondary" noWrap>{request.service} · {request.requestId}</Typography>
              </Box>
              <Chip
                label={request.status}
                size="small"
                color={request.status >= 500 ? 'error' : request.status >= 400 ? 'warning' : 'success'}
              />
              <Typography variant="body2">{request.latencyMs.toFixed(2)} ms</Typography>
              <Button
                size="small"
                startIcon={<ReplayIcon />}
                disabled={!request.replayable || replaying === request.requestId}
                onClick={() => replay(request)}
              >
                Replay
              </Button>
            </Box>
          ))}
        </Box>
      )}
    </Box>
  );
}
