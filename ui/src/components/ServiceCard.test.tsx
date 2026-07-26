import { render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import type { DashboardSettings, Service } from '../hooks/useServices';
import ServiceCard from './ServiceCard';

const settings: DashboardSettings = {
  bq_duckdb: false,
  gke_kind: false,
  serverless_pack: false,
};

function renderCard(service: Service) {
  render(
    <ServiceCard
      service={service}
      idx={0}
      settings={settings}
      onStartContainer={vi.fn()}
      onToggleSetting={vi.fn()}
    />,
  );
}

describe('ServiceCard backend presentation', () => {
  it('derives backend and fidelity labels from backend metadata', () => {
    renderCard({
      id: 'gke',
      name: 'minisky-gke',
      label: 'GKE',
      status: 'RUNNING',
      port: null,
      description: 'Clusters',
      backend: {
        profile: 'full',
        backend: 'kind',
        enabled: true,
        source: 'profile',
      },
    });

    expect(screen.getByText('Backend: kind (enabled)')).toBeInTheDocument();
    expect(screen.getByText('Fidelity: backend-integrated')).toBeInTheDocument();
  });

  it('does not infer fidelity from a published port', () => {
    renderCard({
      id: 'storage',
      name: 'fake-gcs-server',
      label: 'Storage',
      status: 'RUNNING',
      port: 4443,
      description: 'Buckets',
    });

    expect(screen.queryByText(/^Backend:/)).not.toBeInTheDocument();
    expect(screen.queryByText(/^Fidelity:/)).not.toBeInTheDocument();
    expect(screen.getByText('tcp://localhost:4443')).toBeInTheDocument();
  });
});
