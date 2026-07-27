import { useState, useEffect, useCallback, useRef } from 'react';
import { checkedMutation, requireOk, safeRequestError } from '../apiClient';

export type BackendState = {
  profile: string;
  backend: string;
  enabled: boolean;
  source: string;
  diagnostic?: string;
};

export type Service = {
  id: string;
  name: string;
  label: string;
  status: string;
  port: number | null;
  description: string;
  missingDeps?: string[];
  backend?: BackendState;
};

export type DashboardSettings = {
  bq_duckdb: boolean;
  gke_kind: boolean;
  serverless_pack: boolean;
  runtime_profile?: { name: string; diagnostic?: string };
  backends?: Record<string, BackendState>;
};

export type DashboardSettingKey = keyof DashboardSettings;

const defaultSettings: DashboardSettings = {
  bq_duckdb: false,
  gke_kind: false,
  serverless_pack: false,
};

function isDashboardSettings(value: unknown): value is DashboardSettings {
  if (typeof value !== 'object' || value === null) {
    return false;
  }

  const settings = value as Record<string, unknown>;
  const runtimeProfile = settings.runtime_profile;
  const backends = settings.backends;
  return typeof settings.bq_duckdb === 'boolean'
    && typeof settings.gke_kind === 'boolean'
    && typeof settings.serverless_pack === 'boolean'
    && (runtimeProfile === undefined || (
      typeof runtimeProfile === 'object'
      && runtimeProfile !== null
      && typeof (runtimeProfile as Record<string, unknown>).name === 'string'
      && ((runtimeProfile as Record<string, unknown>).diagnostic === undefined
        || typeof (runtimeProfile as Record<string, unknown>).diagnostic === 'string')
    ))
    && (backends === undefined || (
      typeof backends === 'object'
      && backends !== null
      && !Array.isArray(backends)
      && Object.values(backends).every(isBackendState)
    ));
}

function isBackendState(value: unknown): value is BackendState {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return false;
  const backend = value as Record<string, unknown>;
  return typeof backend.profile === 'string'
    && typeof backend.backend === 'string'
    && typeof backend.enabled === 'boolean'
    && typeof backend.source === 'string'
    && (backend.diagnostic === undefined || typeof backend.diagnostic === 'string');
}

function isService(value: unknown): value is Service {
  if (typeof value !== 'object' || value === null) return false;
  const service = value as Record<string, unknown>;
  return typeof service.id === 'string'
    && typeof service.name === 'string'
    && typeof service.label === 'string'
    && typeof service.status === 'string'
    && (typeof service.port === 'number' || service.port === null)
    && typeof service.description === 'string'
    && (service.missingDeps === undefined || (
      Array.isArray(service.missingDeps) && service.missingDeps.every(item => typeof item === 'string')
    ))
    && (service.backend === undefined || isBackendState(service.backend));
}

export function useServices() {
  const [services, setServices] = useState<Service[]>([]);
  const [settings, setSettings] = useState<DashboardSettings>(defaultSettings);
  const [error, setError] = useState<string | null>(null);
  const [hasLoaded, setHasLoaded] = useState(false);
  const readControllerRef = useRef<AbortController | undefined>(undefined);
  const generationRef = useRef(0);
  const timerRef = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const mountedRef = useRef(false);
  const refreshRef = useRef<() => Promise<void>>(async () => undefined);
  const mutationQueueRef = useRef<Promise<void>>(Promise.resolve());

  const fetchData = useCallback(async (signal: AbortSignal, generation: number) => {
    const res = await fetch('/api/services', { signal });
    await requireOk(res, 'Unable to load service status.');
    const serviceData: unknown = await res.json();
    if (!Array.isArray(serviceData) || !serviceData.every(isService)) {
      throw new Error('Service status returned an invalid response.');
    }
    const setRes = await fetch('/api/settings', { signal });
    await requireOk(setRes, 'Unable to load runtime settings.');
    const data: unknown = await setRes.json();
    if (!isDashboardSettings(data)) {
      throw new Error('Runtime settings returned an invalid response.');
    }
    if (generation !== generationRef.current || signal.aborted) return;
    setServices(serviceData);
    setSettings(data);
    setHasLoaded(true);
    setError(null);
  }, []);

  const loadData = useCallback(async () => {
    if (timerRef.current) clearTimeout(timerRef.current);
    readControllerRef.current?.abort();
    const controller = new AbortController();
    readControllerRef.current = controller;
    const generation = ++generationRef.current;
    try {
      await fetchData(controller.signal, generation);
    } catch (cause: unknown) {
      if (cause instanceof DOMException && cause.name === 'AbortError') return;
      if (generation === generationRef.current) {
        setError(cause instanceof Error ? cause.message : 'Unable to load dashboard data');
      }
    } finally {
      if (mountedRef.current && generation === generationRef.current) {
        timerRef.current = setTimeout(() => void refreshRef.current(), 3000);
      }
    }
  }, [fetchData]);
  refreshRef.current = loadData;

  const runMutation = useCallback((
    work: () => Promise<void>,
    describeError: (cause: unknown) => string,
  ) => {
    const execute = async () => {
      if (timerRef.current) clearTimeout(timerRef.current);
      readControllerRef.current?.abort();
      generationRef.current++;
      try {
        await work();
        if (mountedRef.current) void refreshRef.current();
      } catch (cause: unknown) {
        if (mountedRef.current) {
          setError(describeError(cause));
          timerRef.current = setTimeout(() => void refreshRef.current(), 3000);
        }
      }
    };
    const result = mutationQueueRef.current.then(execute, execute);
    mutationQueueRef.current = result.catch(() => undefined);
    return result;
  }, []);

  useEffect(() => {
    const generation = generationRef;
    mountedRef.current = true;
    void loadData();
    return () => {
      mountedRef.current = false;
      generation.current++;
      if (timerRef.current) clearTimeout(timerRef.current);
      readControllerRef.current?.abort();
    };
  }, [loadData]);

  const handleStartContainer = (id: string, projectID?: string) => {
    let url = `/api/services/${id}/start`;
    if (projectID) {
      url += `?project=${encodeURIComponent(projectID)}`;
    }
    return runMutation(async () => {
      await checkedMutation(url, { method: 'POST' }, 'Service start failed. Check the required backend and retry.');
    }, cause => safeRequestError(cause, 'Unable to connect while starting the service.'));
  };

  const handleStopContainer = (id: string) => runMutation(async () => {
    await checkedMutation(`/api/services/${id}/stop`, { method: 'POST' }, 'Service stop failed. Wait for active operations and retry.');
  }, cause => safeRequestError(cause, 'Unable to connect while stopping the service.'));

  const toggleSetting = (key: DashboardSettingKey, currentVal: boolean) => runMutation(
    async () => {
      await checkedMutation('/api/settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ [key]: !currentVal })
      }, 'Failed to update backend setting.');
    },
    cause => safeRequestError(cause, 'Unable to connect while updating the backend setting.'),
  );

  const handleInstallDependency = (id: string) => runMutation(async () => {
      await checkedMutation(`/api/manage/system/install-dependency/${id}`, { method: 'POST' },
        'Dependency installation failed. Check network access and local permissions.');
      alert(`${id} installed successfully! You can now enable the service.`);
    }, cause => safeRequestError(cause, 'Unable to connect while installing the dependency.'));

  return {
    services,
    settings,
    error,
    hasLoaded,
    stale: hasLoaded && error !== null,
    handleStartContainer,
    handleStopContainer,
    toggleSetting,
    handleInstallDependency,
  };
}
