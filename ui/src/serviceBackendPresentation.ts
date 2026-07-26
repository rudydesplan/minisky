import type { BackendState } from './hooks/useServices';

export type BackendPresentation = {
  backendLabel: string;
  fidelityLabel: string;
};

export function serviceBackendPresentation(backend: BackendState | undefined): BackendPresentation | null {
  if (!backend) return null;

  const integrated = backend.enabled && backend.backend !== 'simulation';
  return {
    backendLabel: `Backend: ${backend.backend} (${backend.enabled ? 'enabled' : 'disabled'})`,
    fidelityLabel: `Fidelity: ${integrated ? 'backend-integrated' : 'simulated'}`,
  };
}
