export function customNetworkPayload(name: string) {
  return { name: name.trim(), autoCreateSubnetworks: false };
}

export function subnetworkPayload(project: string, network: string, name: string, ipCidrRange: string) {
  return {
    name: name.trim(),
    ipCidrRange: ipCidrRange.trim(),
    network: `projects/${project}/global/networks/${network}`,
  };
}
