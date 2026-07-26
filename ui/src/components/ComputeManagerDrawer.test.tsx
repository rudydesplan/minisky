import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { ProjectContext } from '../contexts/projectContextValue';
import ComputeManagerDrawer from './ComputeManagerDrawer';

const projectContext = {
  activeProject: 'local-dev-project',
  setActiveProject: vi.fn(),
  availableProjects: ['local-dev-project'],
  addProject: vi.fn(async () => undefined),
  projectError: null,
};

describe('Compute mutations', () => {
  it('does not report or refresh a failed VM deletion', async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = input.toString();
      if (init?.method === 'DELETE') {
        return new Response('client_secret=hidden', { status: 409 });
      }
      if (url === '/api/config/images') {
        return Response.json({ compute: { os_images: [] } });
      }
      if (url.includes('/global/networks')) {
        return Response.json({ items: [{ name: 'default' }] });
      }
      return Response.json({
        items: [{
          name: 'test-vm',
          id: '1',
          status: 'RUNNING',
          machineType: 'n1-standard-1',
          description: '',
          networkInterfaces: [{ networkIP: '10.0.0.2' }],
          creationTimestamp: '2026-01-01T00:00:00Z',
        }],
      });
    });
    vi.stubGlobal('fetch', fetchMock);

    render(
      <ProjectContext.Provider value={projectContext}>
        <ComputeManagerDrawer open onClose={vi.fn()} />
      </ProjectContext.Provider>,
    );

    await userEvent.click(await screen.findByRole('button', { name: 'Delete instance' }));

    const alert = await screen.findByRole('alert', { hidden: true });
    expect(alert).toHaveTextContent('VM deletion failed. Stop the VM and detach dependent resources before retrying.');
    expect(alert).not.toHaveTextContent('client_secret');
    expect(fetchMock.mock.calls.filter(([, init]) => init?.method === 'DELETE')).toHaveLength(1);
  });
});
