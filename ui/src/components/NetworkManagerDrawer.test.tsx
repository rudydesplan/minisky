import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { ProjectContext } from '../contexts/projectContextValue';
import NetworkManagerDrawer from './NetworkManagerDrawer';

const projectContext = {
  activeProject: 'local-dev-project',
  setActiveProject: vi.fn(),
  availableProjects: ['local-dev-project'],
  addProject: vi.fn(async () => undefined),
  projectError: null,
};

describe('NetworkManagerDrawer DNS mutations', () => {
  it('catches network failures and renders only a safe message', async () => {
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      if (init?.method === 'POST') {
        throw new Error('https://private.test/dns?api_key=secret client_secret=hidden /private/path');
      }
      const url = input.toString();
      const body = url.includes('/managedZones')
        ? { managedZones: [] }
        : { items: [] };
      return new Response(JSON.stringify(body), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }));

    render(
      <ProjectContext.Provider value={projectContext}>
        <NetworkManagerDrawer open onClose={vi.fn()} />
      </ProjectContext.Provider>,
    );

    const user = userEvent.setup();
    await user.click(screen.getByRole('tab', { name: 'Cloud DNS' }));
    await user.type(screen.getByRole('textbox', { name: 'Zone Name' }), 'demo-zone');
    await user.type(screen.getByRole('textbox', { name: 'DNS Name' }), 'example.test.');
    await user.click(screen.getByRole('button', { name: 'Create Zone' }));

    const alert = await screen.findByRole('alert', { hidden: true });
    expect(alert).toHaveTextContent('DNS zone creation failed. Check the local service and retry.');
    expect(alert).not.toHaveTextContent(/api_key|client_secret|https:|\/private/);
  });
});
