import { render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import App from './App';
import { ProjectContext } from './contexts/projectContextValue';

const projectContext = {
  activeProject: 'local-dev-project',
  setActiveProject: vi.fn(),
  availableProjects: ['local-dev-project'],
  addProject: vi.fn(async () => undefined),
  projectError: null,
};

describe('application routing and landmarks', () => {
  beforeEach(() => {
    window.history.replaceState({}, '', '/does-not-exist');
    vi.stubGlobal('fetch', vi.fn(async () =>
      new Response(JSON.stringify({ version: 'test' }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      }),
    ));
  });

  it('renders an accessible client-side 404 and skip link', async () => {
    render(
      <ProjectContext.Provider value={projectContext}>
        <App />
      </ProjectContext.Provider>,
    );

    expect(await screen.findByRole('heading', { name: 'Page not found' })).toBeInTheDocument();
    expect(screen.getByRole('navigation', { name: 'Primary navigation' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Skip to main content' })).toHaveAttribute('href', '#main-content');
    expect(document.querySelector('main')).toHaveAttribute('id', 'main-content');
  });
});
