
import { useEffect, useState } from 'react';
import { Link, Route, Switch, useLocation } from 'wouter';
import { Alert, Box, Button, Drawer, IconButton, List, ListItemButton, ListItemIcon, ListItemText, Typography, useMediaQuery } from '@mui/material';
import DashboardIcon from '@mui/icons-material/Dashboard';
import StorageIcon from '@mui/icons-material/Storage';
import ComputeIcon from '@mui/icons-material/Computer';
import DatabaseIcon from '@mui/icons-material/Storage';
import HubIcon from '@mui/icons-material/Hub';
import TerminalIcon from '@mui/icons-material/Terminal';
import BarChartIcon from '@mui/icons-material/BarChart';
import LocalFireDepartmentIcon from '@mui/icons-material/LocalFireDepartment';
import RocketLaunchIcon from '@mui/icons-material/RocketLaunch';
import ScheduleIcon from '@mui/icons-material/Schedule';
import SecurityIcon from '@mui/icons-material/Security';
import HttpIcon from '@mui/icons-material/Http';
import MenuIcon from '@mui/icons-material/Menu';
import Dashboard from './components/Dashboard';
import StoragePage from './components/StoragePage';
import ComputePage from './components/ComputePage';
import DatabasePage from './components/DatabasePage';
import NetworkPage from './components/NetworkPage';
import ProjectSelector from './components/ProjectSelector';
import LogExplorer from './components/LogExplorer';
import MonitoringPage from './components/MonitoringPage';
import FirebasePage from './components/FirebasePage';
import AppEnginePage from './components/AppEnginePage';
import MemorystorePage from './components/MemorystorePage';
import TasksAndSchedulingPage from './components/TasksAndSchedulingPage';
import SecurityPage from './components/SecurityPage';
import GatewayRequestsPage from './components/GatewayRequestsPage';
import { DASHBOARD_AUTH_EVENT, requireOk } from './apiClient';
import { useProjectContext } from './contexts/ProjectContext';

const DRAWER_WIDTH = 280;

const NAV_ITEMS = [
  { to: '/',           label: 'System Diagnostics',       icon: <DashboardIcon /> },
  { to: '/compute',   label: 'Compute Engine Instances',  icon: <ComputeIcon /> },
  { to: '/storage',   label: 'Data Storage Buckets',      icon: <StorageIcon /> },
  { to: '/database',  label: 'Database Topology',         icon: <DatabaseIcon /> },
  { to: '/network',   label: 'Networking',                icon: <HubIcon /> },
  { to: '/firebase',  label: 'Firebase Services',         icon: <LocalFireDepartmentIcon /> },
  { to: '/appengine', label: 'App Engine',                icon: <RocketLaunchIcon /> },
  { to: '/security',  label: 'Security & Identity',       icon: <SecurityIcon /> },
  { to: '/memorystore', label: 'Memorystore',             icon: <StorageIcon /> },
  { to: '/tasks',       label: 'Integration & CI/CD',     icon: <ScheduleIcon /> },
];

function NavItem({ to, label, icon, onNavigate }: {
  to: string;
  label: string;
  icon: React.ReactNode;
  onNavigate?: () => void;
}) {
  const [pathname] = useLocation();
  const active = pathname === to;
  return (
    <ListItemButton
      component={Link}
      to={to}
      onClick={onNavigate}
      aria-current={active ? 'page' : undefined}
      sx={{
        borderRadius: '8px', mb: 1,
        backgroundColor: active ? '#e8f0fe' : 'transparent',
        '&:hover': { backgroundColor: active ? '#e8f0fe' : '#f1f3f4' }
      }}
    >
      <ListItemIcon sx={{ color: active ? '#1a73e8' : '#5f6368', minWidth: 40 }}>{icon}</ListItemIcon>
      <ListItemText primary={label} sx={{ color: active ? '#1a73e8' : '#3c4043', '& span': { fontWeight: active ? 500 : 400 } }} />
    </ListItemButton>
  );
}

function NavigationContent() {
  const [pathname] = useLocation();
  const { projectError } = useProjectContext();
  const isLogging = pathname === '/logging';
  const isMonitoring = pathname === '/monitoring';
  const isGatewayRequests = pathname === '/gateway-requests';
  const [version, setVersion] = useState('...');
  const [authError, setAuthError] = useState<string | null>(null);
  const compactNavigation = useMediaQuery('(max-width:900px)');
  const [navigationOpen, setNavigationOpen] = useState(false);
  const closeNavigation = () => setNavigationOpen(false);

  useEffect(() => {
    fetch('/api/system/info')
      .then(res => requireOk(res, 'Unable to load MiniSky version information.'))
      .then(res => res.json())
      .then(data => setVersion(data.version))
      .catch(err => console.error('Failed to fetch version:', err));
  }, []);

  useEffect(() => {
    const handleAuthError = (event: Event) => {
      setAuthError((event as CustomEvent<string>).detail);
    };
    window.addEventListener(DASHBOARD_AUTH_EVENT, handleAuthError);
    return () => window.removeEventListener(DASHBOARD_AUTH_EVENT, handleAuthError);
  }, []);

  return (
    <Box sx={{ display: 'flex', minHeight: '100vh' }}>
      <Button
        component="a"
        href="#main-content"
        className="skip-link"
        variant="contained"
      >
        Skip to main content
      </Button>
      {compactNavigation && (
        <IconButton
          aria-label="Open primary navigation"
          onClick={() => setNavigationOpen(true)}
          sx={{ position: 'fixed', top: 8, left: 8, zIndex: theme => theme.zIndex.drawer + 1, backgroundColor: '#fff' }}
        >
          <MenuIcon />
        </IconButton>
      )}
      {/* Sidebar */}
      <Drawer
        variant={compactNavigation ? 'temporary' : 'permanent'}
        open={!compactNavigation || navigationOpen}
        onClose={closeNavigation}
        slotProps={{ paper: { component: 'nav', role: 'navigation', 'aria-label': 'Primary navigation' } }}
        ModalProps={{ keepMounted: true }}
        sx={{
          width: compactNavigation ? 0 : DRAWER_WIDTH,
          flexShrink: 0,
          '& .MuiDrawer-paper': { width: DRAWER_WIDTH, maxWidth: '100vw', boxSizing: 'border-box' },
        }}
      >
        {/* Logo */}
        <Box sx={{ p: 4, display: 'flex', alignItems: 'center', gap: 2, borderBottom: '1px solid #dadce0' }}>
          <img src="/minisky_logo.png" alt="MiniSky Logo" style={{ width: 36, height: 36, objectFit: 'contain' }} />
          <Typography variant="h6" sx={{ letterSpacing: '0.01em', fontWeight: 500, color: '#3c4043' }}>
            MiniSky v{version}
          </Typography>
        </Box>

        {/* Nav */}
        <List sx={{ px: 2, mt: 2 }}>
          {NAV_ITEMS.map(n => <NavItem key={n.to} {...n} onNavigate={closeNavigation} />)}

          {/* Divider before Operations section */}
          <Box sx={{ my: 1.5, mx: 1, borderTop: '1px solid #e0e0e0' }} />
          <Typography variant="caption" sx={{ px: 1, color: '#9aa0a6', fontWeight: 600, letterSpacing: '0.08em', textTransform: 'uppercase', fontSize: '0.65rem' }}>
            Operations
          </Typography>

          {/* Gateway request diagnostics */}
          <ListItemButton
            component={Link}
            to="/gateway-requests"
            onClick={closeNavigation}
            aria-current={isGatewayRequests ? 'page' : undefined}
            sx={{
              borderRadius: '8px', mt: 1,
              backgroundColor: isGatewayRequests ? '#e8f0fe' : 'transparent',
              '&:hover': { backgroundColor: isGatewayRequests ? '#e8f0fe' : '#f1f3f4' }
            }}
          >
            <ListItemIcon sx={{ color: isGatewayRequests ? '#1a73e8' : '#5f6368', minWidth: 40 }}>
              <HttpIcon />
            </ListItemIcon>
            <ListItemText
              primary="Gateway Requests"
              sx={{ color: isGatewayRequests ? '#1a73e8' : '#3c4043', '& span': { fontWeight: isGatewayRequests ? 500 : 400 } }}
            />
          </ListItemButton>

          {/* Cloud Logging */}
          <ListItemButton
            component={Link}
            to="/logging"
            onClick={closeNavigation}
            aria-current={isLogging ? 'page' : undefined}
            sx={{
              borderRadius: '8px', mt: 1,
              backgroundColor: isLogging ? '#e8f0fe' : 'transparent',
              '&:hover': { backgroundColor: isLogging ? '#e8f0fe' : '#f1f3f4' }
            }}
          >
            <ListItemIcon sx={{ color: isLogging ? '#1a73e8' : '#5f6368', minWidth: 40 }}>
              <TerminalIcon />
            </ListItemIcon>
            <ListItemText
              primary="Cloud Logging"
              sx={{ color: isLogging ? '#1a73e8' : '#3c4043', '& span': { fontWeight: isLogging ? 500 : 400 } }}
            />
          </ListItemButton>

          {/* Cloud Monitoring */}
          <ListItemButton
            component={Link}
            to="/monitoring"
            onClick={closeNavigation}
            aria-current={isMonitoring ? 'page' : undefined}
            sx={{
              borderRadius: '8px', mt: 0.5,
              backgroundColor: isMonitoring ? '#e8f0fe' : 'transparent',
              '&:hover': { backgroundColor: isMonitoring ? '#e8f0fe' : '#f1f3f4' }
            }}
          >
            <ListItemIcon sx={{ color: isMonitoring ? '#1a73e8' : '#5f6368', minWidth: 40 }}>
              <BarChartIcon />
            </ListItemIcon>
            <ListItemText
              primary="Container Metrics"
              sx={{ color: isMonitoring ? '#1a73e8' : '#3c4043', '& span': { fontWeight: isMonitoring ? 500 : 400 } }}
            />
          </ListItemButton>
        </List>
      </Drawer>

      {/* Main content */}
      <Box component="main" id="main-content" tabIndex={-1} sx={{
        flexGrow: 1, minWidth: 0,
        // Log Explorer gets full height with no padding
        ...(isLogging || isMonitoring || isGatewayRequests ? {
          p: 0,
          pt: compactNavigation ? 7 : 0,
          boxSizing: 'border-box',
          height: '100vh',
          overflow: 'hidden',
          display: 'flex',
          flexDirection: 'column',
        }
                                          : { p: { xs: 2, md: 6 }, pt: compactNavigation ? 8 : undefined, position: 'relative' })
      }}>
        {!isLogging && !isMonitoring && !isGatewayRequests && <ProjectSelector />}
        {(authError || projectError) && (
          <Alert
            severity="error"
            role="alert"
            onClose={authError ? () => setAuthError(null) : undefined}
            sx={{ mb: 2, mr: isLogging || isMonitoring || isGatewayRequests ? 2 : 0 }}
          >
            {authError ?? projectError}
          </Alert>
        )}
        <Switch>
          <Route path="/" component={Dashboard} />
          <Route path="/compute" component={ComputePage} />
          <Route path="/storage" component={StoragePage} />
          <Route path="/database" component={DatabasePage} />
          <Route path="/network" component={NetworkPage} />
          <Route path="/logging" component={LogExplorer} />
          <Route path="/monitoring" component={MonitoringPage} />
          <Route path="/gateway-requests" component={GatewayRequestsPage} />
          <Route path="/firebase" component={FirebasePage} />
          <Route path="/appengine" component={AppEnginePage} />
          <Route path="/security" component={SecurityPage} />
          <Route path="/memorystore" component={MemorystorePage} />
          <Route path="/tasks" component={TasksAndSchedulingPage} />
          <Route>
            <Box sx={{ maxWidth: 640, py: 8 }}>
              <Typography variant="h3" component="h1" sx={{ mb: 2 }}>Page not found</Typography>
              <Typography sx={{ mb: 3, color: 'text.secondary' }}>
                This dashboard route does not exist.
              </Typography>
              <Button component={Link} to="/" variant="contained">Return to diagnostics</Button>
            </Box>
          </Route>
        </Switch>
      </Box>
    </Box>
  );
}

export default function App() {
  return <NavigationContent />;
}
