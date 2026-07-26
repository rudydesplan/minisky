import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { ProjectContext } from '../contexts/projectContextValue';
import SchedulerPage from './SchedulerPage';

const projectContext = {
  activeProject: 'local-dev-project',
  setActiveProject: vi.fn(),
  availableProjects: ['local-dev-project'],
  addProject: vi.fn(async () => undefined),
  projectError: null,
};

describe('Scheduler actions', () => {
  it('checks failed action responses and shows a shared permission error', async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({
        jobs: [{
          name: 'projects/local-dev-project/locations/us-central1/jobs/nightly',
          schedule: '0 0 * * *',
          state: 'ENABLED',
        }],
      }), { status: 200, headers: { 'content-type': 'application/json' } }))
      .mockResolvedValueOnce(new Response(JSON.stringify({
        error: { message: 'Caller lacks permission' },
      }), { status: 403, headers: { 'content-type': 'application/json' } }));
    vi.stubGlobal('fetch', fetchMock);

    render(
      <ProjectContext.Provider value={projectContext}>
        <SchedulerPage />
      </ProjectContext.Provider>,
    );

    await userEvent.click(await screen.findByRole('button', { name: 'Run nightly now' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('Permission denied');
    expect(fetchMock).toHaveBeenLastCalledWith(
      '/api/manage/scheduler/projects/local-dev-project/locations/us-central1/jobs/nightly:run',
      { method: 'POST' },
    );
  });
});
