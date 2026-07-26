import { describe, expect, it } from 'vitest';
import { artifactPackagesRoute, artifactVersionsRoute } from '../artifactRegistryRoutes';

describe('Artifact Registry management routes', () => {
  it('uses the registered management endpoint without a duplicate API version', () => {
    const repository = 'projects/demo/locations/us-central1/repositories/apps';
    expect(artifactPackagesRoute(repository))
      .toBe('/api/manage/artifactregistry/projects/demo/locations/us-central1/repositories/apps/packages');
  });

  it('encodes nested package IDs on the real versions endpoint', () => {
    const packageName = 'projects/demo/locations/us-central1/repositories/apps/packages/team/api';
    expect(artifactVersionsRoute(packageName))
      .toBe('/api/manage/artifactregistry/projects/demo/locations/us-central1/repositories/apps/packages/team%2Fapi/versions');
  });
});
