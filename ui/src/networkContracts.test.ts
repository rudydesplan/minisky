import { describe, expect, it } from 'vitest';
import { customNetworkPayload, subnetworkPayload } from './networkContracts';

describe('network management contracts', () => {
  it('always creates supported custom-mode VPCs', () => {
    expect(customNetworkPayload(' team-vpc ')).toEqual({
      name: 'team-vpc',
      autoCreateSubnetworks: false,
    });
  });

  it('targets bounded regional subnetworks with canonical VPC names', () => {
    expect(subnetworkPayload('demo', 'team-vpc', ' apps ', ' 10.10.0.0/24 ')).toEqual({
      name: 'apps',
      ipCidrRange: '10.10.0.0/24',
      network: 'projects/demo/global/networks/team-vpc',
    });
  });
});
