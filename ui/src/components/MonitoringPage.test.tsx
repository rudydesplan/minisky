import { render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import MonitoringPage from './MonitoringPage';

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('MonitoringPage responses', () => {
  it('reports malformed successful metric responses', async () => {
    vi.stubGlobal('fetch', vi.fn(async () =>
      new Response(JSON.stringify([{ name: 'container', cpu: 'not-a-number' }]), { status: 200 }),
    ));

    render(<MonitoringPage />);
    expect(await screen.findByRole('alert')).toHaveTextContent(
      'Container metrics returned an invalid response.',
    );
    expect(screen.queryByText('No running containers detected.')).not.toBeInTheDocument();
  });
});
