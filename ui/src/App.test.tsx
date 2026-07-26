import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
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

  it('uses a named temporary navigation control on narrow screens', async () => {
    vi.stubGlobal('matchMedia', vi.fn().mockImplementation((query: string) => ({
      matches: query === '(max-width:900px)',
      media: query,
      onchange: null,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      addListener: vi.fn(),
      removeListener: vi.fn(),
      dispatchEvent: vi.fn(),
    })));
    const user = userEvent.setup();
    render(
      <ProjectContext.Provider value={projectContext}>
        <App />
      </ProjectContext.Provider>,
    );

    await user.click(screen.getByRole('button', { name: 'Open primary navigation' }));
    expect(screen.getByRole('navigation', { name: 'Primary navigation' })).toBeVisible();
  });
});
