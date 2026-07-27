import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  FormControl,
  InputLabel,
  MenuItem,
  Select,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
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
    && (record.traceId === undefined || typeof record.traceId === 'string')
    && Number.isInteger(record.status)
    && Number.isFinite(record.status)
    && (record.status as number) >= 100
    && (record.status as number) <= 599
    && typeof record.latencyMs === 'number'
    && Number.isFinite(record.latencyMs)
    && record.latencyMs >= 0
    && typeof record.replayable === 'boolean';
}

export default function GatewayRequestsPage() {
  const [requests, setRequests] = useState<GatewayRequest[]>([]);
  const [loading, setLoading] = useState(true);
  const [hasLoaded, setHasLoaded] = useState(false);
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
      setHasLoaded(true);
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
    <Box sx={{ p: { xs: 2, sm: 3, md: 4 }, minHeight: '100%', background: '#f8f9fa' }}>
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

      {error && (
        <Alert severity={hasLoaded ? 'warning' : 'error'} sx={{ mb: 2 }}>
          {hasLoaded ? `Showing stale gateway records. ${error}` : error}
        </Alert>
      )}

      <Box sx={{ display: 'flex', gap: 2, mb: 2, flexWrap: 'wrap' }}>
        <TextField
          size="small"
          label="Route, request ID, or trace ID"
          value={search}
          onChange={event => setSearch(event.target.value)}
          sx={{ width: { xs: '100%', sm: 'auto' }, minWidth: { sm: 280 } }}
        />
        <FormControl size="small" sx={{ width: { xs: '100%', sm: 'auto' }, minWidth: { sm: 180 } }}>
          <InputLabel id="gateway-service-label">Service</InputLabel>
          <Select
            labelId="gateway-service-label"
            label="Service"
            value={service}
            onChange={event => setService(event.target.value)}
          >
            {services.map(value => <MenuItem key={value} value={value}>{value === 'ALL' ? 'All services' : value}</MenuItem>)}
          </Select>
        </FormControl>
        <FormControl size="small" sx={{ width: { xs: '100%', sm: 'auto' }, minWidth: { sm: 140 } }}>
          <InputLabel id="gateway-method-label">Method</InputLabel>
          <Select
            labelId="gateway-method-label"
            label="Method"
            value={method}
            onChange={event => setMethod(event.target.value)}
          >
            {['ALL', 'GET', 'POST', 'PUT', 'PATCH', 'DELETE'].map(value => (
              <MenuItem key={value} value={value}>{value === 'ALL' ? 'All methods' : value}</MenuItem>
            ))}
          </Select>
        </FormControl>
        <Chip label={`${filtered.length} requests`} />
      </Box>

      {loading && requests.length === 0 ? (
        <Box sx={{ display: 'grid', placeItems: 'center', py: 10 }}><CircularProgress /></Box>
      ) : hasLoaded && filtered.length === 0 ? (
        <Typography color="text.secondary" sx={{ py: 8, textAlign: 'center' }}>No matching requests.</Typography>
      ) : (
        <TableContainer sx={{ border: '1px solid #dadce0', borderRadius: 2, background: '#fff' }}>
          <Table size="small" sx={{ minWidth: 900 }}>
            <TableHead>
              <TableRow>
                <TableCell>Method</TableCell>
                <TableCell>Route and service</TableCell>
                <TableCell>Trace ID</TableCell>
                <TableCell>Status</TableCell>
                <TableCell>Latency</TableCell>
                <TableCell>Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {filtered.map(request => (
                <TableRow key={request.requestId}>
                  <TableCell><Chip label={request.method} size="small" /></TableCell>
                  <TableCell>
                    <Typography sx={{ fontFamily: 'monospace', fontSize: 13 }} noWrap>{request.route}</Typography>
                    <Typography variant="caption" color="text.secondary" noWrap>
                      {request.service} · {request.requestId}
                    </Typography>
                  </TableCell>
                  <TableCell sx={{ fontFamily: 'monospace', fontSize: 12 }}>
                    {request.traceId ?? '—'}
                  </TableCell>
                  <TableCell>
                    <Chip
                      label={request.status}
                      size="small"
                      color={request.status >= 500 ? 'error' : request.status >= 400 ? 'warning' : 'success'}
                    />
                  </TableCell>
                  <TableCell>{request.latencyMs.toFixed(2)} ms</TableCell>
                  <TableCell>
                    <Button
                      size="small"
                      startIcon={<ReplayIcon />}
                      aria-label={`Replay ${request.method} ${request.requestId}`}
                      disabled={!request.replayable || replaying === request.requestId}
                      onClick={() => replay(request)}
                    >
                      Replay
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      )}
    </Box>
  );
}
