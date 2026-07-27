import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

function source(file) {
  return readFileSync(resolve(process.cwd(), 'src/components', file), 'utf8');
}

describe('compact operational layouts', () => {
  it('does not force Log Explorer controls wider than compact screens', () => {
    const logExplorer = source('LogExplorer.tsx');
    expect(logExplorer).not.toContain('minWidth: 280');
    expect(logExplorer).toContain("width: { xs: '100%', sm: 'auto' }");
  });

  it('allows monitoring cards to shrink below 320 pixels', () => {
    const monitoring = source('MonitoringPage.tsx');
    expect(monitoring).not.toContain('minmax(320px, 1fr)');
    expect(monitoring).toContain('minmax(min(100%, 320px), 1fr)');
  });
});
