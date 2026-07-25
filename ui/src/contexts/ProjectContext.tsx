import React, { useState, useEffect } from 'react';
import { ProjectContext } from './projectContextValue';

export { useProjectContext } from './projectContextValue';

type ProjectResource = {
  projectId: string;
  state: string;
};

function isProjectList(value: unknown): value is { projects: ProjectResource[] } {
  if (typeof value !== 'object' || value === null || !('projects' in value)) return false;
  const projects = (value as { projects?: unknown }).projects;
  return Array.isArray(projects) && projects.every(project =>
    typeof project === 'object' && project !== null &&
    typeof (project as ProjectResource).projectId === 'string' &&
    typeof (project as ProjectResource).state === 'string'
  );
}

const projectProvider = function ProjectProvider({ children }: { children: React.ReactNode }) {
  const [activeProject, setActiveProjectState] = useState<string>('local-dev-project');
  const [availableProjects, setAvailableProjects] = useState<string[]>(['local-dev-project']);
  const [projectError, setProjectError] = useState<string | null>(null);

  useEffect(() => {
    const saved = localStorage.getItem('minisky-active-project');
    const controller = new AbortController();
    fetch('/api/projects', { signal: controller.signal })
      .then(async response => {
        if (!response.ok) throw new Error('Project registry is unavailable');
        const value: unknown = await response.json();
        if (!isProjectList(value)) throw new Error('Project registry returned an invalid response');
        const projects = value.projects.filter(project => project.state === 'ACTIVE').map(project => project.projectId);
        setAvailableProjects(projects);
        setActiveProjectState(saved && projects.includes(saved) ? saved : 'local-dev-project');
        setProjectError(null);
      })
      .catch(error => {
        if (error instanceof DOMException && error.name === 'AbortError') return;
        setProjectError(error instanceof Error ? error.message : 'Unable to load projects');
      });
    return () => controller.abort();
  }, []);

  const setActiveProject = (name: string) => {
    setActiveProjectState(name);
    localStorage.setItem('minisky-active-project', name);
  };

  const addProject = async (name: string) => {
    const response = await fetch('/api/projects', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ projectId: name, displayName: name }),
    });
    if (!response.ok) {
      setProjectError('Project creation failed');
      throw new Error('Project creation failed');
    }
    setAvailableProjects(projects => projects.includes(name) ? projects : [...projects, name]);
    setProjectError(null);
    setActiveProject(name);
  };

  return (
    <ProjectContext.Provider value={{ activeProject, setActiveProject, availableProjects, addProject, projectError }}>
      {children}
    </ProjectContext.Provider>
  );
};

export const ProjectProvider = projectProvider;
