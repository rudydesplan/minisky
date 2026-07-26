export function gkeClusterRoute(project, zone, cluster = '', config = false) {
  const base = `/api/manage/gke/projects/${encodeURIComponent(project)}/zones/${encodeURIComponent(zone)}/clusters`;
  if (!cluster) return base;
  return `${base}/${encodeURIComponent(cluster)}${config ? '/config' : ''}`;
}
