package orchestrator

import (
	"fmt"
	"net/netip"
)

var prohibitedVPCIPv4Prefixes = [...]netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("224.0.0.0/4"),
	netip.MustParsePrefix("240.0.0.0/4"),
}

// NormalizeVPCIPv4Prefix validates the bounded custom-subnetwork CIDR contract.
// The input must already be canonical IPv4 /8 through /29 and must not overlap
// prohibited special-use address space.
func NormalizeVPCIPv4Prefix(value string) (netip.Prefix, error) {
	prefix, err := netip.ParsePrefix(value)
	if err != nil || !prefix.Addr().Is4() || prefix != prefix.Masked() ||
		prefix.String() != value || prefix.Bits() < 8 || prefix.Bits() > 29 {
		return netip.Prefix{}, fmt.Errorf("invalid canonical IPv4 subnetwork CIDR %q", value)
	}
	for _, prohibited := range prohibitedVPCIPv4Prefixes {
		if prefix.Overlaps(prohibited) {
			return netip.Prefix{}, fmt.Errorf(
				"IPv4 subnetwork CIDR %q overlaps prohibited range %s",
				value,
				prohibited,
			)
		}
	}
	return prefix, nil
}
