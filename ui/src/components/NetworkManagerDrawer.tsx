import { useState, useEffect, useCallback } from 'react';
import {
  Drawer, Box, Typography, IconButton, Button,
  Table, TableBody, TableCell, TableHead, TableRow,
  TextField, Chip, Snackbar, Alert, Tabs, Tab,
  Select, MenuItem, FormControl, InputLabel, Divider,
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import DeleteIcon from '@mui/icons-material/Delete';
import AddIcon from '@mui/icons-material/Add';
import DnsIcon from '@mui/icons-material/Dns';
import HubIcon from '@mui/icons-material/Hub';
import { useProjectContext } from '../contexts/ProjectContext';
import { requireOk, safeRequestError } from '../apiClient';
import { customNetworkPayload, subnetworkPayload } from '../networkContracts';

type NetworkManagerDrawerProps = { open: boolean; onClose: () => void };

type VpcNetwork = { name: string; id: string; autoCreateSubnetworks: boolean; creationTimestamp: string };
type Subnetwork = { name: string; ipCidrRange: string; network: string; region: string };
type ManagedZone = { name: string; dnsName: string; visibility: string; creationTime: string; nameServers: string[] };
type RRSet = { name: string; type: string; ttl: number; rrdatas: string[] };

export default function NetworkManagerDrawer({ open, onClose }: NetworkManagerDrawerProps) {
  const { activeProject } = useProjectContext();
  const [tab, setTab] = useState(0);
  const [toast, setToast] = useState({ msg: '', open: false, severity: 'success' as 'success' | 'error' });
  const showToast = (msg: string, severity: 'success' | 'error' = 'success') =>
    setToast({ msg, open: true, severity });

  // ── VPC state ──────────────────────────────────────────────────────────────
  const [networks, setNetworks] = useState<VpcNetwork[]>([]);
  const [newNetName, setNewNetName] = useState('');
  const [subnetworks, setSubnetworks] = useState<Subnetwork[]>([]);
  const [newSubnetName, setNewSubnetName] = useState('');
  const [newSubnetCIDR, setNewSubnetCIDR] = useState('10.10.0.0/24');
  const [newSubnetNetwork, setNewSubnetNetwork] = useState('');
  const region = 'us-central1';

  // ── DNS state ──────────────────────────────────────────────────────────────
  const [zones, setZones] = useState<ManagedZone[]>([]);
  const [selectedZone, setSelectedZone] = useState<ManagedZone | null>(null);
  const [rrsets, setRrsets] = useState<RRSet[]>([]);
  const [newZoneName, setNewZoneName] = useState('');
  const [newDnsName, setNewDnsName] = useState('');
  const [newRRName, setNewRRName] = useState('');
  const [newRRType, setNewRRType] = useState('A');
  const [newRRData, setNewRRData] = useState('');
  const [newRRTtl, setNewRRTtl] = useState('300');

  // ── Loaders ────────────────────────────────────────────────────────────────
  const loadNetworks = useCallback(async () => {
    try {
      const res = await fetch(`/api/manage/network/projects/${activeProject}/global/networks`);
      await requireOk(res, 'Unable to load VPC networks.');
      const data = await res.json();
      setNetworks(data.items || []);
    } catch (e) { showToast(safeRequestError(e, 'Unable to load VPC networks. Check the local service and retry.'), 'error'); }
  }, [activeProject]);

  const loadSubnetworks = useCallback(async () => {
    try {
      const res = await fetch(`/api/manage/network/projects/${activeProject}/regions/${region}/subnetworks`);
      await requireOk(res, 'Unable to load subnetworks.');
      const data = await res.json();
      setSubnetworks(data.items || []);
    } catch (e) { showToast(safeRequestError(e, 'Unable to load subnetworks. Check the local service and retry.'), 'error'); }
  }, [activeProject]);

  const loadZones = useCallback(async () => {
    try {
      const res = await fetch(`/api/manage/dns/projects/${activeProject}/managedZones`);
      if (res.ok) {
        const data = await res.json();
        setZones(data.managedZones || []);
      }
    } catch (e) { console.error(e); }
  }, [activeProject]);

  const loadRRSets = useCallback(async (zoneName: string) => {
    try {
      const res = await fetch(`/api/manage/dns/projects/${activeProject}/managedZones/${zoneName}/rrsets`);
      if (res.ok) {
        const data = await res.json();
        setRrsets(data.rrsets || []);
      }
    } catch (e) { console.error(e); }
  }, [activeProject]);

  useEffect(() => {
    if (open) { loadNetworks(); loadSubnetworks(); loadZones(); }
  }, [open, loadNetworks, loadSubnetworks, loadZones]);

  useEffect(() => {
    if (selectedZone) loadRRSets(selectedZone.name);
  }, [selectedZone, loadRRSets]);

  // ── VPC actions ────────────────────────────────────────────────────────────
  const handleCreateNetwork = async () => {
    if (!newNetName.trim()) return;
    try {
      const res = await fetch(`/api/manage/network/projects/${activeProject}/global/networks`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(customNetworkPayload(newNetName)),
      });
      await requireOk(res, 'Failed to create VPC. Custom-mode networks are required.');
      showToast(`VPC "${newNetName}" created`);
      setNewNetName('');
      loadNetworks();
    } catch (e) { showToast(safeRequestError(e, 'Unable to connect while creating the VPC.'), 'error'); }
  };

  const handleDeleteNetwork = async (name: string) => {
    try {
      const res = await fetch(`/api/manage/network/projects/${activeProject}/global/networks/${name}`, { method: 'DELETE' });
      await requireOk(res, 'Failed to delete VPC. Remove attached subnetworks and instances first.');
      showToast(`VPC "${name}" deleted`);
      loadNetworks();
    } catch (e) { showToast(safeRequestError(e, 'Unable to connect while deleting the VPC.'), 'error'); }
  };

  const handleCreateSubnetwork = async () => {
    if (!newSubnetName.trim() || !newSubnetNetwork || !newSubnetCIDR.trim()) return;
    try {
      const res = await fetch(`/api/manage/network/projects/${activeProject}/regions/${region}/subnetworks`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(subnetworkPayload(activeProject, newSubnetNetwork, newSubnetName, newSubnetCIDR)),
      });
      await requireOk(res, 'Failed to create subnetwork. Check the CIDR and selected VPC.');
      showToast(`Subnetwork "${newSubnetName}" created`);
      setNewSubnetName('');
      loadSubnetworks();
    } catch (e) { showToast(safeRequestError(e, 'Unable to connect while creating the subnetwork.'), 'error'); }
  };

  // ── DNS Zone actions ───────────────────────────────────────────────────────
  const handleCreateZone = async () => {
    if (!newZoneName.trim() || !newDnsName.trim()) return;
    try {
      const res = await fetch(`/api/manage/dns/projects/${activeProject}/managedZones`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: newZoneName, dnsName: newDnsName, visibility: 'public' }),
      });
      await requireOk(res, 'Failed to create zone. Check the zone and DNS names.');
      showToast(`Zone "${newZoneName}" created`); setNewZoneName(''); setNewDnsName(''); loadZones();
    } catch (e) { showToast(safeRequestError(e, 'DNS zone creation failed. Check the local service and retry.'), 'error'); }
  };

  const handleDeleteZone = async (name: string) => {
    try {
      const res = await fetch(`/api/manage/dns/projects/${activeProject}/managedZones/${name}`, { method: 'DELETE' });
      await requireOk(res, 'Failed to delete zone. Remove custom records and retry.');
      showToast(`Zone "${name}" deleted`); setSelectedZone(null); loadZones();
    } catch (e) { showToast(safeRequestError(e, 'DNS zone deletion failed. Check the local service and retry.'), 'error'); }
  };

  // ── RRSet actions ──────────────────────────────────────────────────────────
  const handleAddRecord = async () => {
    if (!selectedZone || !newRRName || !newRRData) return;
    try {
      const res = await fetch(
        `/api/manage/dns/projects/${activeProject}/managedZones/${selectedZone.name}/rrsets`,
        {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: newRRName, type: newRRType, ttl: parseInt(newRRTtl), rrdatas: [newRRData] }),
        },
      );
      await requireOk(res, 'Failed to add DNS record. Check the record values.');
      showToast(`${newRRType} record added`);
      setNewRRName(''); setNewRRData('');
      loadRRSets(selectedZone.name);
    } catch (e) { showToast(safeRequestError(e, 'DNS record creation failed. Check the local service and retry.'), 'error'); }
  };

  const handleDeleteRecord = async (name: string, type: string) => {
    if (!selectedZone) return;
    try {
      const res = await fetch(`/api/manage/dns/projects/${activeProject}/managedZones/${selectedZone.name}/rrsets/${encodeURIComponent(name)}/${type}`, { method: 'DELETE' });
      await requireOk(res, 'Failed to delete DNS record.');
      showToast(`${type} record deleted`);
      loadRRSets(selectedZone.name);
    } catch (e) { showToast(safeRequestError(e, 'DNS record deletion failed. Check the local service and retry.'), 'error'); }
  };

  const recordTypeColor = (t: string): 'primary' | 'secondary' | 'success' | 'warning' | 'error' | 'info' | 'default' => {
    const m: Record<string, 'primary' | 'secondary' | 'success' | 'warning' | 'error' | 'info'> = {
      A: 'primary', AAAA: 'info', CNAME: 'secondary', MX: 'warning', TXT: 'success', NS: 'error',
    };
    return m[t] || 'default';
  };

  return (
    <>
      <Drawer anchor="right" open={open} onClose={onClose}>
        <Box sx={{ width: 800, p: 4 }}>
          {/* Header */}
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
              <HubIcon sx={{ color: '#1a73e8' }} />
              <Typography variant="h5" sx={{ fontWeight: 500 }}>Network Manager</Typography>
            </Box>
            <IconButton aria-label="Close Network Manager" onClick={onClose}><CloseIcon /></IconButton>
          </Box>
          <Chip label={activeProject} size="small" sx={{ mb: 2, backgroundColor: '#e8f0fe', color: '#1a73e8', fontWeight: 600 }} />

          <Tabs value={tab} onChange={(_, v) => setTab(v)} sx={{ mb: 3, borderBottom: '1px solid #dadce0' }}>
            <Tab icon={<HubIcon fontSize="small" />} label="VPC Networks" iconPosition="start" />
            <Tab icon={<DnsIcon fontSize="small" />} label="Cloud DNS" iconPosition="start" />
          </Tabs>

          {/* ── TAB 0: VPC Networks ─────────────────────────────────────────── */}
          {tab === 0 && (
            <Box>
              <Box sx={{ display: 'flex', gap: 1.5, mb: 3, alignItems: 'flex-end' }}>
                <TextField
                  size="small" label="Network Name" placeholder="my-vpc"
                  value={newNetName}
                  onChange={e => setNewNetName(e.target.value.toLowerCase().replace(/[^a-z0-9-]/g, ''))}
                  sx={{ flex: 1 }}
                />
                <FormControl size="small" sx={{ minWidth: 190 }}>
                  <InputLabel>Subnet mode</InputLabel>
                  <Select value="custom" label="Subnet mode" disabled>
                    <MenuItem value="custom">Custom (supported)</MenuItem>
                  </Select>
                </FormControl>
                <Button variant="contained" startIcon={<AddIcon />} onClick={handleCreateNetwork} disabled={!newNetName.trim()}>
                  Create VPC
                </Button>
              </Box>
              <Alert severity="info" sx={{ mb: 3 }}>
                Auto-mode VPC creation is not supported. MiniSky creates custom-mode VPCs; add a bounded regional subnetwork below.
              </Alert>

              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Network Name</TableCell>
                    <TableCell>Subnets</TableCell>
                    <TableCell>Created</TableCell>
                    <TableCell align="right">Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {networks.length === 0 && (
                    <TableRow><TableCell colSpan={4} align="center" sx={{ color: '#80868b', py: 4 }}>No VPC networks. Create one above.</TableCell></TableRow>
                  )}
                  {networks.map(n => (
                    <TableRow key={n.name} hover>
                      <TableCell sx={{ fontWeight: 600 }}>{n.name}</TableCell>
                      <TableCell><Chip size="small" label={n.autoCreateSubnetworks ? 'Auto' : 'Custom'} variant="outlined" /></TableCell>
                      <TableCell sx={{ color: '#80868b', fontSize: '0.8rem' }}>{new Date(n.creationTimestamp).toLocaleDateString()}</TableCell>
                      <TableCell align="right">
                        <IconButton aria-label={`Delete VPC ${n.name}`} size="small" color="error" onClick={() => handleDeleteNetwork(n.name)}><DeleteIcon fontSize="small" /></IconButton>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>

              <Typography variant="h6" component="h3" sx={{ mt: 4, mb: 2 }}>Subnetworks ({region})</Typography>
              <Box sx={{ display: 'flex', gap: 1.5, mb: 2, alignItems: 'flex-end', flexWrap: 'wrap' }}>
                <TextField size="small" label="Subnetwork name" value={newSubnetName}
                  onChange={e => setNewSubnetName(e.target.value.toLowerCase().replace(/[^a-z0-9-]/g, ''))} />
                <TextField size="small" label="Primary IPv4 CIDR" value={newSubnetCIDR}
                  onChange={e => setNewSubnetCIDR(e.target.value)} />
                <FormControl size="small" sx={{ minWidth: 180 }}>
                  <InputLabel>VPC network</InputLabel>
                  <Select value={newSubnetNetwork} label="VPC network" inputProps={{ 'aria-label': 'VPC network' }} onChange={e => setNewSubnetNetwork(e.target.value)}>
                    {networks.filter(network => !network.autoCreateSubnetworks).map(network => (
                      <MenuItem key={network.name} value={network.name}>{network.name}</MenuItem>
                    ))}
                  </Select>
                </FormControl>
                <Button variant="outlined" onClick={handleCreateSubnetwork}
                  disabled={!newSubnetName || !newSubnetCIDR || !newSubnetNetwork}>Create subnetwork</Button>
              </Box>
              <Table size="small" aria-label="Subnetworks">
                <TableHead><TableRow><TableCell>Name</TableCell><TableCell>CIDR</TableCell><TableCell>VPC</TableCell></TableRow></TableHead>
                <TableBody>
                  {subnetworks.map(subnetwork => (
                    <TableRow key={`${subnetwork.region}/${subnetwork.name}`}>
                      <TableCell>{subnetwork.name}</TableCell>
                      <TableCell sx={{ fontFamily: 'monospace' }}>{subnetwork.ipCidrRange}</TableCell>
                      <TableCell>{subnetwork.network.split('/').pop()}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </Box>
          )}

          {/* ── TAB 1: Cloud DNS ─────────────────────────────────────────────── */}
          {tab === 1 && (
            <Box>
              {selectedZone ? (
                <>
                  {/* RRSet detail view */}
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
                    <Button size="small" onClick={() => setSelectedZone(null)}>← Back to Zones</Button>
                    <Typography variant="h6">{selectedZone.name}</Typography>
                    <Chip label={selectedZone.dnsName} size="small" sx={{ ml: 1 }} />
                  </Box>

                  {/* Add record form */}
                  <Box sx={{ display: 'flex', gap: 1, mb: 3, flexWrap: 'wrap', alignItems: 'flex-end' }}>
                    <TextField size="small" label="Record Name (FQDN)" placeholder="www.example.com." value={newRRName} onChange={e => setNewRRName(e.target.value)} sx={{ flex: 2, minWidth: 180 }} />
                    <FormControl size="small" sx={{ minWidth: 90 }}>
                      <InputLabel>Type</InputLabel>
                      <Select value={newRRType} label="Type" onChange={e => setNewRRType(e.target.value)}>
                        {['A', 'AAAA', 'CNAME', 'MX', 'TXT', 'NS', 'PTR', 'SRV'].map(t => <MenuItem key={t} value={t}>{t}</MenuItem>)}
                      </Select>
                    </FormControl>
                    <TextField size="small" label="TTL (s)" value={newRRTtl} onChange={e => setNewRRTtl(e.target.value)} sx={{ width: 80 }} />
                    <TextField size="small" label="Data" placeholder="1.2.3.4" value={newRRData} onChange={e => setNewRRData(e.target.value)} sx={{ flex: 2, minWidth: 150 }} />
                    <Button variant="contained" startIcon={<AddIcon />} onClick={handleAddRecord} disabled={!newRRName || !newRRData}>Add Record</Button>
                  </Box>

                  <Table size="small">
                    <TableHead>
                      <TableRow>
                        <TableCell>Name</TableCell>
                        <TableCell>Type</TableCell>
                        <TableCell>TTL</TableCell>
                        <TableCell>Data</TableCell>
                        <TableCell align="right">Actions</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {rrsets.map(rr => (
                        <TableRow key={rr.name + rr.type} hover>
                          <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.8rem' }}>{rr.name}</TableCell>
                          <TableCell><Chip label={rr.type} size="small" color={recordTypeColor(rr.type)} /></TableCell>
                          <TableCell sx={{ color: '#80868b' }}>{rr.ttl}s</TableCell>
                          <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.8rem', maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis' }}>
                            {rr.rrdatas?.join(', ')}
                          </TableCell>
                          <TableCell align="right">
                            {rr.type !== 'SOA' && rr.type !== 'NS' && (
                              <IconButton aria-label={`Delete ${rr.type} record ${rr.name}`} size="small" color="error" onClick={() => handleDeleteRecord(rr.name, rr.type)}><DeleteIcon fontSize="small" /></IconButton>
                            )}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </>
              ) : (
                <>
                  {/* Zone list view */}
                  <Box sx={{ display: 'flex', gap: 1.5, mb: 3, alignItems: 'flex-end' }}>
                    <TextField size="small" label="Zone Name" placeholder="my-zone" value={newZoneName} onChange={e => setNewZoneName(e.target.value.toLowerCase().replace(/[^a-z0-9-]/g, ''))} sx={{ flex: 1 }} />
                    <TextField size="small" label="DNS Name" placeholder="example.com." value={newDnsName} onChange={e => setNewDnsName(e.target.value)} sx={{ flex: 1 }} />
                    <Button variant="contained" startIcon={<AddIcon />} onClick={handleCreateZone} disabled={!newZoneName || !newDnsName}>Create Zone</Button>
                  </Box>

                  <Divider sx={{ mb: 2 }} />

                  <Table size="small">
                    <TableHead>
                      <TableRow>
                        <TableCell>Zone Name</TableCell>
                        <TableCell>DNS Name</TableCell>
                        <TableCell>Visibility</TableCell>
                        <TableCell>Name Servers</TableCell>
                        <TableCell align="right">Actions</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {zones.length === 0 && (
                        <TableRow><TableCell colSpan={5} align="center" sx={{ color: '#80868b', py: 4 }}>No DNS zones. Create one above.</TableCell></TableRow>
                      )}
                      {zones.map(z => (
                        <TableRow key={z.name} hover sx={{ cursor: 'pointer' }} onClick={() => setSelectedZone(z)}>
                          <TableCell sx={{ fontWeight: 600, color: '#1a73e8' }}>{z.name}</TableCell>
                          <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.8rem' }}>{z.dnsName}</TableCell>
                          <TableCell><Chip size="small" label={z.visibility} color={z.visibility === 'public' ? 'success' : 'default'} variant="outlined" /></TableCell>
                          <TableCell sx={{ color: '#80868b', fontSize: '0.75rem' }}>{z.nameServers?.[0]}</TableCell>
                          <TableCell align="right">
                            <IconButton aria-label={`Delete DNS zone ${z.name}`} size="small" color="error" onClick={e => { e.stopPropagation(); handleDeleteZone(z.name); }}><DeleteIcon fontSize="small" /></IconButton>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </>
              )}
            </Box>
          )}
        </Box>
      </Drawer>

      <Snackbar open={toast.open} autoHideDuration={3000} onClose={() => setToast(t => ({ ...t, open: false }))} anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}>
        <Alert severity={toast.severity} sx={{ width: '100%' }}>{toast.msg}</Alert>
      </Snackbar>
    </>
  );
}
