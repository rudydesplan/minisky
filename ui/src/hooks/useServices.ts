import { useState, useEffect, useCallback } from 'react';
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
  return typeof settings.bq_duckdb === 'boolean'
    && typeof settings.gke_kind === 'boolean'
    && typeof settings.serverless_pack === 'boolean';
}

export function useServices() {
  const [services, setServices] = useState<Service[]>([]);
  const [settings, setSettings] = useState<DashboardSettings>(defaultSettings);
  const [error, setError] = useState<string | null>(null);

  const loadData = useCallback(async () => {
    try {
      const res = await fetch('/api/services');
      await requireOk(res, 'Unable to load service status.');
      setServices(await res.json() as Service[]);
      const setRes = await fetch('/api/settings');
      await requireOk(setRes, 'Unable to load runtime settings.');
      const data: unknown = await setRes.json();
      if (isDashboardSettings(data)) {
        setSettings(data);
        setError(null);
      } else {
        throw new Error('Runtime settings returned an invalid response.');
      }
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Unable to load dashboard data');
    }
  }, []);

  useEffect(() => {
    loadData();
    const interval = setInterval(loadData, 3000);
    return () => clearInterval(interval);
  }, [loadData]);

  const handleStartContainer = async (id: string, projectID?: string) => {
    let url = `/api/services/${id}/start`;
    if (projectID) {
      url += `?project=${encodeURIComponent(projectID)}`;
    }
    try {
      await checkedMutation(url, { method: 'POST' }, 'Service start failed. Check the required backend and retry.');
      await loadData();
    } catch (cause: unknown) {
      setError(safeRequestError(cause, 'Unable to connect while starting the service.'));
    }
  };

  const handleStopContainer = async (id: string) => {
    try {
      await checkedMutation(`/api/services/${id}/stop`, { method: 'POST' }, 'Service stop failed. Wait for active operations and retry.');
      await loadData();
    } catch (cause: unknown) {
      setError(safeRequestError(cause, 'Unable to connect while stopping the service.'));
    }
  };

  const toggleSetting = async (key: DashboardSettingKey, currentVal: boolean) => {
    try {
      await checkedMutation('/api/settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ [key]: !currentVal })
      }, 'Failed to update backend setting.');
      loadData();
    } catch (e: unknown) {
      setError(safeRequestError(e, 'Unable to connect while updating the backend setting.'));
    }
  };

  const handleInstallDependency = async (id: string) => {
    try {
      await checkedMutation(`/api/manage/system/install-dependency/${id}`, { method: 'POST' },
        'Dependency installation failed. Check network access and local permissions.');
      alert(`${id} installed successfully! You can now enable the service.`);
      loadData();
    } catch (e: unknown) {
      setError(safeRequestError(e, 'Unable to connect while installing the dependency.'));
    }
  };

  return { services, settings, error, handleStartContainer, handleStopContainer, toggleSetting, handleInstallDependency };
}
