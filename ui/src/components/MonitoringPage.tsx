import { useCallback, useState, useEffect } from 'react';
import {
  Box, Typography, Chip, IconButton, Tooltip, CircularProgress,
  Alert, Paper, LinearProgress
} from '@mui/material';
import RefreshIcon from '@mui/icons-material/Refresh';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import StopIcon from '@mui/icons-material/Stop';
import MemoryIcon from '@mui/icons-material/Memory';
import SpeedIcon from '@mui/icons-material/Speed';
import MonitorHeartIcon from '@mui/icons-material/MonitorHeart';
import CodeIcon from '@mui/icons-material/Code';
import StorageIcon from '@mui/icons-material/Storage';
import DeleteSweepIcon from '@mui/icons-material/DeleteSweep';
import { requireOk } from '../apiClient';

type ContainerMetrics = {
  name: string;
  status: string;
  cpu: number;
  memMB: number;
};

type HistoryPoint = { time: string; cpu: number; mem: number };

const MAX_HISTORY = 30;

function resourceIcon(name: string) {
  if (name.includes('serverless')) return <CodeIcon sx={{ fontSize: 16 }} />;
  if (name.includes('compute')) return <MemoryIcon sx={{ fontSize: 16 }} />;
  if (name.includes('storage')) return <StorageIcon sx={{ fontSize: 16 }} />;
  return <MonitorHeartIcon sx={{ fontSize: 16 }} />;
}

function shortName(name: string) {
  return name.replace('minisky-', '').replace('serverless-', 'fn:').replace('compute-', 'vm:');
}

function statusColor(status: string) {
  if (status.startsWith('Up')) return '#81c995';
  if (status.startsWith('Exited')) return '#f28b82';
  return '#fbbc04';
}

function isContainerMetrics(value: unknown): value is ContainerMetrics {
  if (typeof value !== 'object' || value === null) return false;
  const metric = value as Record<string, unknown>;
  return typeof metric.name === 'string'
    && typeof metric.status === 'string'
    && typeof metric.cpu === 'number'
    && Number.isFinite(metric.cpu)
    && typeof metric.memMB === 'number'
    && Number.isFinite(metric.memMB);
}

function MiniSparkline({ points, color }: { points: number[]; color: string }) {
  const width = 120;
  const height = 36;
  if (points.length < 2) return <Box sx={{ width, height, opacity: 0.3 }}>—</Box>;

  const max = Math.max(...points, 0.1);
  const coords = points.map((v, i) => {
    const x = (i / (points.length - 1)) * width;
    const y = height - (v / max) * (height - 4) - 2;
    return `${x},${y}`;
  }).join(' ');

  return (
    <svg width={width} height={height} style={{ overflow: 'visible' }} role="img" aria-label="Recent metric trend">
      <polyline
        points={coords}
        fill="none"
        stroke={color}
        strokeWidth="1.5"
        strokeLinejoin="round"
        strokeLinecap="round"
        opacity="0.9"
      />
      <polyline
        points={`0,${height} ${coords} ${width},${height}`}
        fill={color}
        opacity="0.12"
      />
    </svg>
  );
}

function MetricGauge({ value, max, label, unit, color }: {
  value: number; max: number; label: string; unit: string; color: string;
}) {
  const pct = Math.min((value / max) * 100, 100);
  return (
    <Box sx={{ mb: 1 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 0.3 }}>
        <Typography sx={{ fontSize: '0.72rem', color: '#8b949e' }}>{label}</Typography>
        <Typography sx={{ fontSize: '0.72rem', color, fontWeight: 600 }}>
          {value.toFixed(1)}{unit}
        </Typography>
      </Box>
      <LinearProgress
        variant="determinate"
        value={pct}
        sx={{
          height: 5, borderRadius: 3,
          backgroundColor: '#21262d',
          '& .MuiLinearProgress-bar': { backgroundColor: color, borderRadius: 3 }
        }}
      />
    </Box>
  );
}

export default function MonitoringPage() {
  const [metrics, setMetrics] = useState<ContainerMetrics[]>([]);
  const [history, setHistory] = useState<Record<string, HistoryPoint[]>>({});
  const [loading, setLoading] = useState(false);
  const [streaming, setStreaming] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasLoaded, setHasLoaded] = useState(false);

  const fetchMetrics = useCallback(async (signal?: AbortSignal) => {
    setLoading(true);
    try {
      const res = await fetch('/api/manage/monitoring/stats', { signal });
      await requireOk(res, 'Unable to load container metrics. Verify that Docker is available.');
      const data: unknown = await res.json();
      if (!Array.isArray(data) || !data.every(isContainerMetrics)) {
        throw new Error('Container metrics returned an invalid response.');
      }
      setMetrics(data);
      setHasLoaded(true);
      setError(null);
      const now = new Date().toLocaleTimeString();
      setHistory(prev => {
        const next = { ...prev };
        data.forEach(m => {
          const pts = prev[m.name] ?? [];
          next[m.name] = [
            ...pts,
            { time: now, cpu: m.cpu, mem: m.memMB }
          ].slice(-MAX_HISTORY);
        });
        return next;
      });
    } catch (cause: unknown) {
      if (cause instanceof DOMException && cause.name === 'AbortError') return;
      setError(cause instanceof Error ? cause.message : 'Unable to load container metrics');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    let stopped = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let controller: AbortController | undefined;
    const poll = async () => {
      controller = new AbortController();
      await fetchMetrics(controller.signal);
      if (!stopped && streaming) timer = setTimeout(poll, 1000);
    };
    void poll();
    return () => {
      stopped = true;
      if (timer) clearTimeout(timer);
      controller?.abort();
    };
  }, [fetchMetrics, streaming]);

  const startStream = () => {
    setStreaming(true);
  };

  const stopStream = () => {
    setStreaming(false);
  };

  const handlePrune = async () => {
    if (!window.confirm("Remove all exited containers and unused MiniSky images?")) return;
    setLoading(true);
    try {
      await requireOk(
        await fetch('/api/manage/system/prune-containers', { method: 'POST' }),
        'Container cleanup failed. Check Docker availability and retry.',
      );
      await fetchMetrics();
    } catch (cause: unknown) {
      setError(cause instanceof Error ? cause.message : 'Container cleanup failed');
    } finally {
      setLoading(false);
    }
  };

  const running = metrics.filter(m => m.status.startsWith('Up'));
  const stopped = metrics.filter(m => !m.status.startsWith('Up'));

  return (
    <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column', background: '#0d1117', color: '#c9d1d9', overflow: 'auto' }}>
      {/* Header */}
      <Box sx={{
        px: 3, py: 2,
        background: 'linear-gradient(135deg, #1a1f2e 0%, #161b27 100%)',
        borderBottom: '1px solid #30363d',
        display: 'flex', alignItems: 'center', gap: 2, flexWrap: 'wrap',
        position: 'sticky', top: 0, zIndex: 10
      }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
          <Box sx={{
            width: 36, height: 36, borderRadius: '10px',
            background: 'linear-gradient(135deg, #388e3c, #1b5e20)',
            display: 'flex', alignItems: 'center', justifyContent: 'center'
          }}>
            <MonitorHeartIcon sx={{ color: '#fff', fontSize: 18 }} />
          </Box>
          <Box>
            <Typography variant="h6" sx={{ fontWeight: 700, fontSize: '1rem', color: '#e6edf3', lineHeight: 1 }}>
              Container Metrics
            </Typography>
            <Typography variant="caption" sx={{ color: '#8b949e' }}>
              Docker runtime CPU and memory only — this is not Cloud Monitoring or PromQL
            </Typography>
          </Box>
        </Box>

        <Box sx={{ flex: 1 }} />

        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Chip label={`${running.length} running`} size="small"
            sx={{ background: '#81c99520', color: '#81c995', fontSize: '0.7rem', borderRadius: '6px' }} />
          <Chip label={`${stopped.length} stopped`} size="small"
            sx={{ background: '#f28b8220', color: '#f28b82', fontSize: '0.7rem', borderRadius: '6px' }} />
          {streaming && (
            <Chip label="● LIVE" size="small"
              sx={{ background: '#81c99520', color: '#81c995', fontSize: '0.7rem',
                animation: 'pulse 2s infinite', '@keyframes pulse': { '0%,100%': { opacity: 1 }, '50%': { opacity: 0.5 } } }} />
          )}
        </Box>

        <Tooltip title={streaming ? 'Stop auto-refresh' : 'Auto-refresh (1s)'}>
          <IconButton size="small" onClick={streaming ? stopStream : startStream}
            aria-label={streaming ? 'Stop automatic metrics refresh' : 'Start automatic metrics refresh'}
            sx={{ color: streaming ? '#f28b82' : '#81c995', border: '1px solid', borderColor: 'currentColor' }}>
            {streaming ? <StopIcon fontSize="small" /> : <PlayArrowIcon fontSize="small" />}
          </IconButton>
        </Tooltip>
        <Tooltip title="Prune exited containers & unused images">
          <IconButton size="small" onClick={handlePrune} disabled={loading}
            aria-label="Prune exited containers and unused images"
            sx={{ color: '#f28b82', border: '1px solid', borderColor: '#f28b8240' }}>
            <DeleteSweepIcon fontSize="small" />
          </IconButton>
        </Tooltip>
        <Tooltip title="Refresh now">
          <IconButton size="small" onClick={() => void fetchMetrics()} disabled={loading}
            aria-label="Refresh container metrics"
            sx={{ color: '#8b949e' }}>
            {loading ? <CircularProgress size={16} sx={{ color: '#8b949e' }} /> : <RefreshIcon fontSize="small" />}
          </IconButton>
        </Tooltip>
      </Box>
      {error && (
        <Alert severity={hasLoaded ? 'warning' : 'error'} role="alert" sx={{ m: 2 }}>
          {hasLoaded ? `Showing stale container metrics. ${error}` : error}
        </Alert>
      )}

      {/* Content */}
      {hasLoaded && !error && metrics.length === 0 ? (
        <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', flex: 1, gap: 2 }}>
          <MonitorHeartIcon sx={{ fontSize: 56, color: '#21262d' }} />
          <Typography sx={{ color: '#8b949e' }}>No running containers detected.</Typography>
          <Typography variant="caption" sx={{ color: '#6e7681' }}>
            Start a Cloud Function or Compute instance to see metrics here.
          </Typography>
        </Box>
      ) : metrics.length > 0 ? (
        <Box sx={{ p: { xs: 1.5, sm: 3 } }}>
          <Box sx={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(min(100%, 320px), 1fr))', gap: 2, minWidth: 0 }}>
            {metrics.map(m => {
              const hist = history[m.name] ?? [];
              const cpuHist = hist.map(p => p.cpu);
              const memHist = hist.map(p => p.mem);
              const isUp = m.status.startsWith('Up');

              return (
                <Box key={m.name} sx={{ minWidth: 0 }}>
                  <Paper sx={{
                    background: '#161b27', border: '1px solid #21262d', borderRadius: '12px',
                    p: 2, position: 'relative', overflow: 'hidden',
                    '&:hover': { borderColor: '#30363d', background: '#1a1f2e' },
                    transition: 'all 0.2s'
                  }}>
                    {/* Status bar */}
                    <Box sx={{
                      position: 'absolute', top: 0, left: 0, right: 0, height: 3,
                      background: isUp
                        ? 'linear-gradient(90deg, #81c995, #1e88e5)'
                        : 'linear-gradient(90deg, #f28b82, #ef5350)'
                    }} />

                    {/* Header */}
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2, mt: 0.5 }}>
                      <Box sx={{
                        width: 28, height: 28, borderRadius: '7px',
                        background: isUp ? '#81c99515' : '#f28b8215',
                        display: 'flex', alignItems: 'center', justifyContent: 'center',
                        color: isUp ? '#81c995' : '#f28b82'
                      }}>
                        {resourceIcon(m.name)}
                      </Box>
                      <Box sx={{ flex: 1, minWidth: 0 }}>
                        <Typography sx={{ fontSize: '0.82rem', fontWeight: 600, color: '#e6edf3', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                          {shortName(m.name)}
                        </Typography>
                        <Typography sx={{ fontSize: '0.67rem', color: statusColor(m.status) }}>
                          {m.status.startsWith('Up') ? 'Running' : m.status}
                        </Typography>
                      </Box>
                    </Box>

                    {/* Gauges */}
                    <MetricGauge value={m.cpu} max={100} label="CPU" unit="%" color="#1e88e5" />
                    <MetricGauge value={m.memMB} max={512} label="Memory" unit=" MB" color="#ab47bc" />

                    {/* Sparklines */}
                    {hist.length > 1 && (
                      <Box sx={{ mt: 1.5, display: 'flex', gap: 1, flexWrap: 'wrap' }}>
                        <Box>
                          <Typography sx={{ fontSize: '0.62rem', color: '#6e7681', mb: 0.25 }}>CPU trend</Typography>
                          <MiniSparkline points={cpuHist} color="#1e88e5" />
                        </Box>
                        <Box>
                          <Typography sx={{ fontSize: '0.62rem', color: '#6e7681', mb: 0.25 }}>Mem trend</Typography>
                          <MiniSparkline points={memHist} color="#ab47bc" />
                        </Box>
                      </Box>
                    )}

                    {/* Current values */}
                    <Box sx={{ mt: 1.5, display: 'flex', gap: 1, flexWrap: 'wrap' }}>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                        <SpeedIcon sx={{ fontSize: 13, color: '#1e88e5' }} />
                        <Typography sx={{ fontSize: '0.72rem', color: '#8b949e' }}>
                          {m.cpu.toFixed(2)}% CPU
                        </Typography>
                      </Box>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                        <MemoryIcon sx={{ fontSize: 13, color: '#ab47bc' }} />
                        <Typography sx={{ fontSize: '0.72rem', color: '#8b949e' }}>
                          {m.memMB.toFixed(1)} MB RAM
                        </Typography>
                      </Box>
                    </Box>
                  </Paper>
                </Box>
              );
            })}
          </Box>
        </Box>
      ) : null}
    </Box>
  );
}
