import { useState, useEffect, useCallback } from 'react';

export type Service = {
  id: string;
  name: string;
  label: string;
  status: string;
  port: number | null;
  description: string;
  missingDeps?: string[];
};

export type DashboardSettings = {
  bq_duckdb: boolean;
  gke_kind: boolean;
  serverless_pack: boolean;
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

  const loadData = useCallback(async () => {
    try {
      const res = await fetch('/api/services');
      if (res.ok) {
        setServices(await res.json() as Service[]);
      }
      const setRes = await fetch('/api/settings');
      if (setRes.ok) {
        const data: unknown = await setRes.json();
        if (isDashboardSettings(data)) {
          setSettings(data);
        } else {
          console.error('invalid settings response', data);
        }
      }
    } catch (e) {
      console.error("error loading UI data", e);
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
    await fetch(url, { method: 'POST' });
    loadData();
  };

  const handleStopContainer = async (id: string) => {
    await fetch(`/api/services/${id}/stop`, { method: 'POST' });
    loadData();
  };

  const toggleSetting = async (key: DashboardSettingKey, currentVal: boolean) => {
    try {
      const res = await fetch('/api/settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ [key]: !currentVal })
      });
      if (!res.ok) {
        const errText = await res.text();
        alert(`Failed to update setting: ${errText}`);
      }
      loadData();
    } catch (e: unknown) {
      const message = e instanceof Error ? e.message : String(e);
      alert(`Error updating setting: ${message}`);
    }
  };

  const handleInstallDependency = async (id: string) => {
    try {
      const res = await fetch(`/api/manage/system/install-dependency/${id}`, { method: 'POST' });
      if (!res.ok) {
        const errText = await res.text();
        alert(`Installation failed: ${errText}`);
      } else {
        alert(`${id} installed successfully! You can now enable the service.`);
      }
      loadData();
    } catch (e: unknown) {
      const message = e instanceof Error ? e.message : String(e);
      alert(`Error installing dependency: ${message}`);
    }
  };

  return { services, settings, handleStartContainer, handleStopContainer, toggleSetting, handleInstallDependency };
}
