const TOKEN_KEY = 'minisky-dashboard-token';
const PROJECT_KEY = 'minisky-active-project';
export const DASHBOARD_AUTH_EVENT = 'minisky:dashboard-auth-error';

class DashboardRequestError extends Error {
  override name = 'DashboardRequestError';
}

const SAFE_STATUS_MESSAGES: Readonly<Record<number, string>> = {
  400: 'The request was rejected. Check the entered values and retry.',
  404: 'The requested resource was not found. Refresh the page and retry.',
  409: 'The resource conflicts with existing state. Refresh the page and retry.',
  412: 'The request prerequisites are not satisfied. Refresh the page and retry.',
  422: 'The entered values are invalid. Correct them and retry.',
  429: 'Too many requests were sent. Wait briefly and retry.',
  500: 'MiniSky could not complete the request. Check service status and retry.',
  502: 'A required local backend returned an invalid response. Check backend status and retry.',
  503: 'A required local service is unavailable. Start it and retry.',
  504: 'A required local backend timed out. Check backend status and retry.',
};

export async function responseErrorMessage(response: Response, fallback: string): Promise<string> {
  if (response.status === 401) {
    return 'Authentication required. Add a valid dashboard token in the project menu and retry.';
  }
  if (response.status === 403) {
    return 'Permission denied. Use a dashboard token with access to this project and action.';
  }
  // The response body is intentionally never read. Callers provide trusted,
  // static operation guidance; status guidance is used only without one.
  return fallback.trim()
    || SAFE_STATUS_MESSAGES[response.status]
    || 'The dashboard request failed. Refresh the page and retry.';
}

export async function requireOk(response: Response, fallback: string): Promise<Response> {
  if (!response.ok) throw new DashboardRequestError(await responseErrorMessage(response, fallback));
  return response;
}

export type MutationMethod = 'POST' | 'PUT' | 'PATCH' | 'DELETE';

export async function checkedMutation(
  input: RequestInfo | URL,
  init: RequestInit & { method: MutationMethod },
  guidance: string,
): Promise<Response> {
  return requireOk(await fetch(input, init), guidance);
}

export function safeRequestError(error: unknown, fallback: string): string {
  return error instanceof DashboardRequestError ? error.message : fallback;
}

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
    return nativeFetch(input, { ...init, headers }).then(response => {
      if (response.status === 401 || response.status === 403) {
        void responseErrorMessage(response, 'Dashboard request was rejected').then(message => {
          window.dispatchEvent(new CustomEvent(DASHBOARD_AUTH_EVENT, { detail: message }));
        });
      }
      return response;
    });
  };
}
