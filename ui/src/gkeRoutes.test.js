import test from 'node:test';
import assert from 'node:assert/strict';
import { gkeClusterRoute } from './gkeRoutes.js';

test('builds canonical project and zone GKE routes', () => {
  assert.equal(
    gkeClusterRoute('team-project', 'us-central1-c', 'demo cluster', true),
    '/api/manage/gke/projects/team-project/zones/us-central1-c/clusters/demo%20cluster/config',
  );
  assert.equal(
    gkeClusterRoute('other-project', 'europe-west1-b'),
    '/api/manage/gke/projects/other-project/zones/europe-west1-b/clusters',
  );
});
