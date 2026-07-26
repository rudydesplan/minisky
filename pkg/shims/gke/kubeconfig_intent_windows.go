//go:build windows

package gke

func writeKubeconfigIntent(ClusterIdentity, *kubeconfigOwnership, kubeconfigIntentPhase) error {
	return errSecureKubeconfigUnsupported
}
func writeKubeconfigIntentError(ClusterIdentity, *kubeconfigOwnership, kubeconfigIntentPhase, string) error {
	return errSecureKubeconfigUnsupported
}
func prepareKubeconfigWithIntent(ClusterIdentity) (*secureKubeconfigTarget, *kubeconfigOwnership, error) {
	return nil, nil, errSecureKubeconfigUnsupported
}

func loadKubeconfigIntent(ClusterIdentity) (*kubeconfigIntent, error) {
	return nil, errSecureKubeconfigUnsupported
}

func loadAllKubeconfigIntents(string) ([]kubeconfigIntent, error) {
	return nil, errSecureKubeconfigUnsupported
}
