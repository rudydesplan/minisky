import { createContext, useContext } from 'react';

export type ProjectContextType = {
  activeProject: string;
  setActiveProject: (name: string) => void;
  availableProjects: string[];
  addProject: (name: string) => Promise<void>;
  projectError: string | null;
};

export const ProjectContext = createContext<ProjectContextType | undefined>(undefined);

export function useProjectContext() {
  const context = useContext(ProjectContext);
  if (context === undefined) {
    throw new Error('useProjectContext must be used within a ProjectProvider');
  }
  return context;
}
