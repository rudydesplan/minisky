import React, { useState, useEffect } from 'react';
import { ProjectContext } from './projectContextValue';
import { requireOk, safeRequestError } from '../apiClient';

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
        await requireOk(response, 'Project registry is unavailable. Verify that the Resource Manager shim is running.');
        let value: unknown;
        try {
          value = await response.json();
        } catch {
          throw new Error('Project registry returned an invalid response. Start the MiniSky management API and retry.');
        }
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
    try {
      await requireOk(await fetch('/api/projects', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ projectId: name, displayName: name }),
      }), 'Project creation failed. Check the project ID and retry.');
      setAvailableProjects(projects => projects.includes(name) ? projects : [...projects, name]);
      setProjectError(null);
      setActiveProject(name);
    } catch (error) {
      const message = safeRequestError(error, 'Unable to connect while creating the project.');
      setProjectError(message);
      throw new Error(message, { cause: error });
    }
  };

  return (
    <ProjectContext.Provider value={{ activeProject, setActiveProject, availableProjects, addProject, projectError }}>
      {children}
    </ProjectContext.Provider>
  );
};

export const ProjectProvider = projectProvider;
