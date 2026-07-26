import { expect, test } from 'vitest';
import { gkeClusterRoute } from './gkeRoutes.js';

test('builds canonical project and zone GKE routes', () => {
  expect(
    gkeClusterRoute('team-project', 'us-central1-c', 'demo cluster', true),
  ).toBe('/api/manage/gke/projects/team-project/zones/us-central1-c/clusters/demo%20cluster/config');
  expect(
    gkeClusterRoute('other-project', 'europe-west1-b'),
  ).toBe('/api/manage/gke/projects/other-project/zones/europe-west1-b/clusters');
});
