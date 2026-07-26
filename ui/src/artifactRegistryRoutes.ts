export function artifactPackagesRoute(repositoryName: string): string {
  return `/api/manage/artifactregistry/${repositoryName}/packages`;
}

export function artifactVersionsRoute(packageName: string): string {
  const marker = '/packages/';
  const position = packageName.indexOf(marker);
  if (position < 0) throw new Error('Artifact package name is invalid');
  const parent = packageName.slice(0, position + marker.length);
  const packageID = packageName.slice(position + marker.length);
  return `/api/manage/artifactregistry/${parent}${encodeURIComponent(packageID)}/versions`;
}
