import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { ThemeProvider, CssBaseline } from '@mui/material'
import { googleTheme } from './theme.ts'
import { ProjectProvider } from './contexts/ProjectContext'
import { installDashboardFetch } from './apiClient'
import './index.css'
import App from './App.tsx'

installDashboardFetch()

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <ThemeProvider theme={googleTheme}>
      <CssBaseline />
      <ProjectProvider>
        <App />
      </ProjectProvider>
    </ThemeProvider>
  </StrictMode>,
)
