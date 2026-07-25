const TOKEN_KEY = 'minisky-dashboard-token';
const PROJECT_KEY = 'minisky-active-project';

export function getDashboardToken(): string {
  return sessionStorage.getItem(TOKEN_KEY)?.trim() ?? '';
}

export function setDashboardToken(token: string): void {
  const normalized = token.trim();
  if (normalized) {
    sessionStorage.setItem(TOKEN_KEY, normalized);
  } else {
    sessionStorage.removeItem(TOKEN_KEY);
  }
}

export function installDashboardFetch(): void {
  const nativeFetch = window.fetch.bind(window);
  window.fetch = (input: RequestInfo | URL, init?: RequestInit) => {
    const request = input instanceof Request ? input : undefined;
    const url = new URL(request?.url ?? input.toString(), window.location.href);
    if (url.origin !== window.location.origin || !url.pathname.startsWith('/api/')) {
      return nativeFetch(input, init);
    }

    const headers = new Headers(request?.headers);
    new Headers(init?.headers).forEach((value, key) => headers.set(key, value));
    const token = getDashboardToken();
    if (token) headers.set('Authorization', `Bearer ${token}`);

    // Canonical path/body targets take precedence. This header supplies the
    // selected project only for dashboard operations without such a target.
    if (!/\/projects\/[^/]+/.test(url.pathname) && !(url.pathname === '/api/projects' && init?.method === 'POST')) {
      headers.set('X-MiniSky-Project', localStorage.getItem(PROJECT_KEY) ?? 'local-dev-project');
    }
    return nativeFetch(input, { ...init, headers });
  };
}
