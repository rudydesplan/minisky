package compute

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"minisky/pkg/orchestrator"
)

const maxSubnetworkRequestBytes = 1 << 20

var gceResourceName = regexp.MustCompile(`^[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?$`)

// Subnetwork is the bounded regional Compute control-plane model. READY means
// metadata is committed and the exact owned parent-VPC Docker bridge has matching
// IPv4 IPAM. Workload attachment to that bridge remains outside this phase.
type Subnetwork struct {
	Kind                  string `json:"kind"`
	ID                    string `json:"id"`
	Name                  string `json:"name"`
	Description           string `json:"description,omitempty"`
	IPCidrRange           string `json:"ipCidrRange"`
	Network               string `json:"network"`
	Region                string `json:"region"`
	SelfLink              string `json:"selfLink"`
	GatewayAddress        string `json:"gatewayAddress,omitempty"`
	Fingerprint           string `json:"fingerprint"`
	PrivateIPGoogleAccess bool   `json:"privateIpGoogleAccess"`
	Purpose               string `json:"purpose"`
	StackType             string `json:"stackType"`
	State                 string `json:"state"`
	CreationTimestamp     string `json:"creationTimestamp"`
}

type subnetworkInsertRequest struct {
	Name                  string `json:"name"`
	Description           string `json:"description"`
	IPCidrRange           string `json:"ipCidrRange"`
	Network               string `json:"network"`
	PrivateIPGoogleAccess bool   `json:"privateIpGoogleAccess"`
	Purpose               string `json:"purpose"`
	StackType             string `json:"stackType"`
}

func (api *API) routeSubnetworks(w http.ResponseWriter, r *http.Request, path string) {
	project, region, name, collection, ok := parseSubnetworkPath(path)
	if !ok {
		writeErrorStatus(w, http.StatusNotFound, "NOT_FOUND", "Compute resource not found: "+path)
		return
	}

	if collection {
		switch r.Method {
		case http.MethodPost:
			api.insertSubnetwork(w, r, project, region)
		case http.MethodGet:
			api.listSubnetworks(w, r, project, region)
		case http.MethodPatch, http.MethodPut:
			writeErrorStatus(w, http.StatusNotImplemented, "UNIMPLEMENTED", "Subnetwork updates are not implemented")
		default:
			writeErrorStatus(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed")
		}
		return
	}

	switch r.Method {
	case http.MethodGet:
		api.getSubnetwork(w, project, region, name)
	case http.MethodDelete:
		api.deleteSubnetwork(w, r, project, region, name)
	case http.MethodPatch, http.MethodPut:
		writeErrorStatus(w, http.StatusNotImplemented, "UNIMPLEMENTED", "Subnetwork updates are not implemented")
	default:
		writeErrorStatus(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed")
	}
}

func (api *API) insertSubnetwork(w http.ResponseWriter, r *http.Request, project, region string) {
	request, status, symbolic, err := decodeSubnetworkInsert(w, r)
	if err != nil {
		writeErrorStatus(w, status, symbolic, err.Error())
		return
	}
	networkName, err := resolveSubnetworkNetwork(project, request.Network)
	if err != nil {
		writeErrorStatus(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	prefix, err := validateSubnetworkRequest(request)
	if err != nil {
		var unsupported *unsupportedSubnetworkFieldError
		if errors.As(err, &unsupported) {
			writeErrorStatus(w, http.StatusNotImplemented, "UNIMPLEMENTED", err.Error())
		} else {
			writeErrorStatus(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		}
		return
	}

	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	api.mu.Lock()
	network := api.networks[project+":"+networkName]
	if network == nil || network.Name == "default" || network.AutoCreateSubnetworks {
		api.mu.Unlock()
		writeErrorStatus(w, http.StatusBadRequest, "INVALID_ARGUMENT",
			"network must reference an existing custom-mode network in the same project")
		return
	}
	networkLink := networkSelfLink(project, networkName)
	key := subnetworkKey(project, region, request.Name)
	if _, exists := api.subnetworks[key]; exists {
		api.mu.Unlock()
		writeErrorStatus(w, http.StatusConflict, "ALREADY_EXISTS", "Subnetwork "+request.Name+" already exists")
		return
	}
	for existingKey, existing := range api.subnetworks {
		if existing == nil || !strings.HasPrefix(existingKey, project+":") {
			continue
		}
		if existing.Name == request.Name {
			api.mu.Unlock()
			writeErrorStatus(w, http.StatusConflict, "ALREADY_EXISTS", "Subnetwork "+request.Name+" already exists")
			return
		}
		if existing.Network == networkLink {
			api.mu.Unlock()
			writeErrorStatus(w, http.StatusConflict, "ALREADY_EXISTS",
				"the bounded slice supports exactly one subnetwork per network")
			return
		}
		existingPrefix, parseErr := netip.ParsePrefix(existing.IPCidrRange)
		if parseErr == nil && prefix.Overlaps(existingPrefix) {
			api.mu.Unlock()
			writeErrorStatus(w, http.StatusConflict, "ALREADY_EXISTS",
				"subnetwork IP ranges must not overlap within a project")
			return
		}
	}
	api.mu.Unlock()

	op, err := api.registerRegionalSubnetworkOperation(
		"insert",
		subnetworkSelfLink(project, region, request.Name),
		region,
	)
	if err != nil {
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}
	identity := orchestrator.VPCNetworkIdentity{Project: project, Network: networkName}
	if api.vpcIPAM == nil {
		_ = api.opMgr.FailDurable(op.Name, http.StatusServiceUnavailable, "VPC IPAM backend is unavailable")
		writeErrorStatus(w, http.StatusServiceUnavailable, "FAILED_PRECONDITION", "VPC IPAM backend is unavailable")
		return
	}
	bridge, err := api.vpcIPAM.EnsureVPCNetworkIPAM(r.Context(), identity, prefix.String())
	if err != nil {
		_ = api.opMgr.FailDurable(op.Name, http.StatusServiceUnavailable, "VPC IPAM ensure failed")
		writeErrorStatus(w, http.StatusServiceUnavailable, "FAILED_PRECONDITION",
			"VPC IPAM backend could not ensure the exact owned bridge")
		return
	}

	api.mu.Lock()
	id := api.nextSubnetworkID
	api.nextSubnetworkID++
	subnetwork := &Subnetwork{
		Kind:                  "compute#subnetwork",
		ID:                    strconv.FormatUint(id, 10),
		Name:                  request.Name,
		Description:           request.Description,
		IPCidrRange:           prefix.String(),
		Network:               networkLink,
		Region:                regionSelfLink(project, region),
		SelfLink:              subnetworkSelfLink(project, region, request.Name),
		GatewayAddress:        prefix.Addr().Next().String(),
		PrivateIPGoogleAccess: false,
		Purpose:               "PRIVATE",
		StackType:             "IPV4_ONLY",
		State:                 "READY",
		CreationTimestamp:     time.Now().UTC().Format(time.RFC3339),
	}
	subnetwork.Fingerprint = subnetworkFingerprint(subnetwork)
	api.subnetworks[key] = subnetwork
	payload, snapshotErr := api.marshalMetadataLocked()
	api.mu.Unlock()
	saveErr := snapshotErr
	if saveErr == nil {
		saveErr = api.saveMetadataPayload(payload)
	}
	if saveErr != nil {
		api.mu.Lock()
		delete(api.subnetworks, key)
		api.nextSubnetworkID = id
		api.mu.Unlock()
		cleanupErr := error(nil)
		if bridge.Created {
			cleanupErr = api.vpcIPAM.DeleteVPCNetworkIPAM(context.Background(), identity, prefix.String())
			if errors.Is(cleanupErr, orchestrator.ErrVPCNetworkNotFound) {
				cleanupErr = nil
			}
		} else {
			api.setInitializationError(fmt.Errorf(
				"create subnetwork metadata save failed after reconciling a preexisting exact bridge: %w",
				saveErr,
			))
		}
		_ = api.opMgr.FailDurable(op.Name, http.StatusInternalServerError, "subnetwork metadata commit failed")
		message := "persist subnetwork metadata: " + saveErr.Error()
		if cleanupErr != nil {
			message += "; cleanup exact owned bridge failed"
			api.setInitializationError(fmt.Errorf(
				"create subnetwork metadata save failed: %w; bridge cleanup failed: %v",
				saveErr,
				cleanupErr,
			))
		}
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
		return
	}

	if err := api.opMgr.AdvanceDurable(op.Name, 100, orchestrator.StatusDone); err != nil {
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", "persist terminal operation metadata")
		return
	}
	op = api.opMgr.Get(op.Name)
	w.WriteHeader(http.StatusOK)
	writeComputeOperation(w, project, op)
}

func (api *API) getSubnetwork(w http.ResponseWriter, project, region, name string) {
	api.mu.RLock()
	subnetwork := cloneSubnetwork(api.subnetworks[subnetworkKey(project, region, name)])
	api.mu.RUnlock()
	if subnetwork == nil {
		writeErrorStatus(w, http.StatusNotFound, "NOT_FOUND", "Subnetwork "+name+" not found")
		return
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(subnetwork)
}

func (api *API) listSubnetworks(w http.ResponseWriter, r *http.Request, project, region string) {
	maxResults := 500
	if raw := r.URL.Query().Get("maxResults"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 0 || value > 500 {
			writeErrorStatus(w, http.StatusBadRequest, "INVALID_ARGUMENT", "maxResults must be between 0 and 500")
			return
		}
		if value > 0 {
			maxResults = value
		}
	}
	lastName, err := decodeSubnetworkPageToken(project, region, r.URL.Query().Get("pageToken"))
	if err != nil {
		writeErrorStatus(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}

	prefix := project + ":" + region + ":"
	api.mu.RLock()
	items := make([]*Subnetwork, 0)
	for key, subnetwork := range api.subnetworks {
		if strings.HasPrefix(key, prefix) && subnetwork != nil {
			items = append(items, cloneSubnetwork(subnetwork))
		}
	}
	api.mu.RUnlock()
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	start := 0
	if lastName != "" {
		start = sort.Search(len(items), func(index int) bool {
			return items[index].Name > lastName
		})
	}
	end := start + maxResults
	if end > len(items) {
		end = len(items)
	}
	response := struct {
		Kind          string        `json:"kind"`
		Items         []*Subnetwork `json:"items"`
		NextPageToken string        `json:"nextPageToken,omitempty"`
	}{
		Kind:  "compute#subnetworkList",
		Items: items[start:end],
	}
	if end < len(items) {
		response.NextPageToken = encodeSubnetworkPageToken(project, region, items[end-1].Name)
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(response)
}

func (api *API) deleteSubnetwork(w http.ResponseWriter, r *http.Request, project, region, name string) {
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	key := subnetworkKey(project, region, name)
	api.mu.Lock()
	subnetwork := cloneSubnetwork(api.subnetworks[key])
	if subnetwork == nil {
		api.mu.Unlock()
		writeErrorStatus(w, http.StatusNotFound, "NOT_FOUND", "Subnetwork "+name+" not found")
		return
	}
	for _, instance := range api.instances {
		if instance == nil || len(instance.NetworkInterfaces) == 0 {
			continue
		}
		referencesSubnetwork := false
		for _, networkInterface := range instance.NetworkInterfaces {
			if networkInterface.Subnetwork == subnetwork.SelfLink ||
				(networkInterface.Subnetwork == "" && networkInterface.Network == subnetwork.Network) {
				referencesSubnetwork = true
				break
			}
		}
		if referencesSubnetwork {
			api.mu.Unlock()
			writeErrorStatus(
				w,
				http.StatusBadRequest,
				"FAILED_PRECONDITION",
				"Subnetwork "+name+" is in use by instance "+instance.Name,
			)
			return
		}
	}
	api.mu.Unlock()

	op, err := api.registerRegionalSubnetworkOperation("delete", subnetwork.SelfLink, region)
	if err != nil {
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}
	identity := orchestrator.VPCNetworkIdentity{Project: project, Network: extractNameFromURL(subnetwork.Network)}
	if api.vpcIPAM == nil {
		_ = api.opMgr.FailDurable(op.Name, http.StatusServiceUnavailable, "VPC IPAM backend is unavailable")
		writeErrorStatus(w, http.StatusServiceUnavailable, "FAILED_PRECONDITION", "VPC IPAM backend is unavailable")
		return
	}
	if err := api.vpcIPAM.DeleteVPCNetworkIPAM(r.Context(), identity, subnetwork.IPCidrRange); err != nil {
		_ = api.opMgr.FailDurable(op.Name, http.StatusServiceUnavailable, "VPC IPAM delete failed")
		writeErrorStatus(w, http.StatusServiceUnavailable, "FAILED_PRECONDITION",
			"VPC IPAM backend could not delete the exact owned bridge")
		return
	}

	api.mu.Lock()
	delete(api.subnetworks, key)
	payload, snapshotErr := api.marshalMetadataLocked()
	api.mu.Unlock()
	if snapshotErr != nil {
		api.mu.Lock()
		api.subnetworks[key] = subnetwork
		api.mu.Unlock()
		_, recreateErr := api.vpcIPAM.EnsureVPCNetworkIPAM(context.Background(), identity, subnetwork.IPCidrRange)
		_ = api.opMgr.FailDurable(op.Name, http.StatusInternalServerError, "subnetwork metadata commit failed")
		message := "snapshot subnetwork metadata: " + snapshotErr.Error()
		if recreateErr != nil {
			message += "; recreate exact owned bridge failed"
			api.setInitializationError(fmt.Errorf(
				"delete subnetwork metadata snapshot failed: %w; bridge recreation failed: %v",
				snapshotErr,
				recreateErr,
			))
		}
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
		return
	}
	if err := api.saveMetadataPayload(payload); err != nil {
		api.mu.Lock()
		api.subnetworks[key] = subnetwork
		api.mu.Unlock()
		_, recreateErr := api.vpcIPAM.EnsureVPCNetworkIPAM(context.Background(), identity, subnetwork.IPCidrRange)
		_ = api.opMgr.FailDurable(op.Name, http.StatusInternalServerError, "subnetwork metadata commit failed")
		message := "persist subnetwork deletion: " + err.Error()
		if recreateErr != nil {
			message += "; recreate exact owned bridge failed"
			api.setInitializationError(fmt.Errorf(
				"delete subnetwork metadata save failed: %w; bridge recreation failed: %v",
				err,
				recreateErr,
			))
		}
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
		return
	}

	if err := api.opMgr.AdvanceDurable(op.Name, 100, orchestrator.StatusDone); err != nil {
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", "persist terminal operation metadata")
		return
	}
	op = api.opMgr.Get(op.Name)
	w.WriteHeader(http.StatusOK)
	writeComputeOperation(w, project, op)
}

func (api *API) insertNetwork(w http.ResponseWriter, r *http.Request, project string) {
	var body struct {
		Name                  string `json:"name"`
		Description           string `json:"description"`
		AutoCreateSubnetworks bool   `json:"autoCreateSubnetworks"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErrorStatus(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid network JSON")
		return
	}
	if body.AutoCreateSubnetworks {
		writeErrorStatus(w, http.StatusNotImplemented, "UNIMPLEMENTED",
			"auto-mode VPC networks are not implemented")
		return
	}
	network := &Network{
		Kind:                  "compute#network",
		ID:                    randomNumericID(),
		Name:                  body.Name,
		Description:           body.Description,
		AutoCreateSubnetworks: body.AutoCreateSubnetworks,
		SelfLink:              networkSelfLink(project, body.Name),
		CreationTimestamp:     time.Now().UTC().Format(time.RFC3339),
	}
	key := project + ":" + body.Name

	api.persistMu.Lock()
	api.mu.Lock()
	if api.networks[key] != nil {
		api.mu.Unlock()
		api.persistMu.Unlock()
		writeErrorStatus(w, http.StatusConflict, "ALREADY_EXISTS", "Network "+body.Name+" already exists")
		return
	}
	api.networks[key] = network
	payload, snapshotErr := api.marshalMetadataLocked()
	api.mu.Unlock()
	if snapshotErr != nil {
		api.mu.Lock()
		delete(api.networks, key)
		api.mu.Unlock()
		api.persistMu.Unlock()
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", "snapshot network metadata: "+snapshotErr.Error())
		return
	}
	if err := api.saveMetadataPayload(payload); err != nil {
		api.mu.Lock()
		delete(api.networks, key)
		api.mu.Unlock()
		api.persistMu.Unlock()
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", "persist network metadata: "+err.Error())
		return
	}
	api.persistMu.Unlock()

	if api.opMgr == nil {
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", "operation manager is unavailable")
		return
	}
	op, err := api.opMgr.RegisterDurable("compute#operation", "insert", network.SelfLink, "", "")
	if err != nil {
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}
	api.opMgr.RunAsync(op.Name, func() error { return nil })
	w.WriteHeader(http.StatusOK)
	writeComputeOperation(w, project, op)
}

func (api *API) deleteNetwork(w http.ResponseWriter, r *http.Request, project, name string) {
	api.persistMu.Lock()
	key := project + ":" + name
	networkLink := networkSelfLink(project, name)
	api.mu.Lock()
	network := api.networks[key]
	if network == nil {
		api.mu.Unlock()
		api.persistMu.Unlock()
		writeErrorStatus(w, http.StatusNotFound, "NOT_FOUND", "Network "+name+" not found")
		return
	}
	for subnetworkKey, subnetwork := range api.subnetworks {
		if strings.HasPrefix(subnetworkKey, project+":") &&
			subnetwork != nil && subnetwork.Network == networkLink {
			api.mu.Unlock()
			api.persistMu.Unlock()
			writeErrorStatus(
				w,
				http.StatusBadRequest,
				"FAILED_PRECONDITION",
				"Network "+name+" is in use by subnetwork "+subnetwork.Name,
			)
			return
		}
	}
	for _, instance := range api.instances {
		if instance == nil {
			continue
		}
		referencesNetwork := false
		for _, networkInterface := range instance.NetworkInterfaces {
			if networkInterface.Network == networkLink {
				referencesNetwork = true
				break
			}
		}
		if referencesNetwork {
			api.mu.Unlock()
			api.persistMu.Unlock()
			writeErrorStatus(
				w,
				http.StatusBadRequest,
				"FAILED_PRECONDITION",
				"Network "+name+" is in use by instance "+instance.Name,
			)
			return
		}
	}
	sameNameNetworks := 0
	for _, candidate := range api.networks {
		if candidate != nil && candidate.Name == name {
			sameNameNetworks++
		}
	}
	api.mu.Unlock()
	if sameNameNetworks == 1 {
		if api.legacyVPC == nil {
			api.persistMu.Unlock()
			writeErrorStatus(w, http.StatusServiceUnavailable, "FAILED_PRECONDITION",
				"legacy VPC cleanup backend is unavailable")
			return
		}
		if err := api.legacyVPC.DeleteLegacyVPCNetwork(r.Context(), name); err != nil {
			api.persistMu.Unlock()
			writeErrorStatus(w, http.StatusBadRequest, "FAILED_PRECONDITION",
				"legacy VPC bridge cleanup failed: "+err.Error())
			return
		}
	}
	api.mu.Lock()
	delete(api.networks, key)
	payload, snapshotErr := api.marshalMetadataLocked()
	api.mu.Unlock()
	if snapshotErr != nil {
		api.mu.Lock()
		api.networks[key] = network
		api.mu.Unlock()
		api.persistMu.Unlock()
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", "snapshot network metadata: "+snapshotErr.Error())
		return
	}
	if err := api.saveMetadataPayload(payload); err != nil {
		api.mu.Lock()
		api.networks[key] = network
		api.mu.Unlock()
		api.persistMu.Unlock()
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", "persist network deletion: "+err.Error())
		return
	}
	api.persistMu.Unlock()

	if api.opMgr == nil {
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", "operation manager is unavailable")
		return
	}
	op, err := api.opMgr.RegisterDurable("compute#operation", "delete", network.SelfLink, "", "")
	if err != nil {
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}
	api.opMgr.RunAsync(op.Name, func() error { return nil })
	w.WriteHeader(http.StatusOK)
	writeComputeOperation(w, project, op)
}

func decodeSubnetworkInsert(w http.ResponseWriter, r *http.Request) (subnetworkInsertRequest, int, string, error) {
	var request subnetworkInsertRequest
	body := http.MaxBytesReader(w, r.Body, maxSubnetworkRequestBytes)
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return request, http.StatusRequestEntityTooLarge, "INVALID_ARGUMENT",
				errors.New("request body exceeds 1 MiB")
		}
		return request, http.StatusBadRequest, "INVALID_ARGUMENT", fmt.Errorf("invalid subnetwork JSON: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return request, http.StatusBadRequest, "INVALID_ARGUMENT", errors.New("request body must contain exactly one JSON object")
	}
	return request, 0, "", nil
}

type unsupportedSubnetworkFieldError struct{ message string }

func (err *unsupportedSubnetworkFieldError) Error() string { return err.message }

func validateSubnetworkRequest(request subnetworkInsertRequest) (netip.Prefix, error) {
	if !gceResourceName.MatchString(request.Name) {
		return netip.Prefix{}, errors.New("name must match GCE RFC1035 naming rules")
	}
	if request.IPCidrRange == "" {
		return netip.Prefix{}, errors.New("ipCidrRange is required")
	}
	prefix, err := orchestrator.NormalizeVPCIPv4Prefix(request.IPCidrRange)
	if err != nil {
		return netip.Prefix{}, errors.New("ipCidrRange must be a usable IPv4 /8 through /29")
	}
	if request.Network == "" {
		return netip.Prefix{}, errors.New("network is required")
	}
	if request.PrivateIPGoogleAccess {
		return netip.Prefix{}, &unsupportedSubnetworkFieldError{"privateIpGoogleAccess changes are not implemented"}
	}
	if request.Purpose != "" && request.Purpose != "PRIVATE" {
		return netip.Prefix{}, &unsupportedSubnetworkFieldError{"only purpose PRIVATE is supported"}
	}
	if request.StackType != "" && request.StackType != "IPV4_ONLY" {
		return netip.Prefix{}, &unsupportedSubnetworkFieldError{"only stackType IPV4_ONLY is supported"}
	}
	return prefix, nil
}

func resolveSubnetworkNetwork(project, value string) (string, error) {
	if gceResourceName.MatchString(value) {
		return value, nil
	}
	const marker = "https://www.googleapis.com/compute/v1/projects/"
	if !strings.HasPrefix(value, marker) {
		return "", errors.New("network must be a name or canonical Compute network self-link")
	}
	remaining := strings.TrimPrefix(value, marker)
	parts := strings.Split(remaining, "/")
	if len(parts) != 4 || parts[0] != project || parts[1] != "global" ||
		parts[2] != "networks" || !gceResourceName.MatchString(parts[3]) {
		return "", errors.New("network self-link must reference the same project")
	}
	return parts[3], nil
}

func parseSubnetworkPath(path string) (project, region, name string, collection, ok bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 7 && len(parts) != 8 {
		return "", "", "", false, false
	}
	if parts[0] != "compute" || parts[1] != "v1" || parts[2] != "projects" ||
		parts[3] == "" || parts[4] != "regions" || parts[5] == "" || parts[6] != "subnetworks" {
		return "", "", "", false, false
	}
	if len(parts) == 8 {
		if parts[7] == "" {
			return "", "", "", false, false
		}
		return parts[3], parts[5], parts[7], false, true
	}
	return parts[3], parts[5], "", true, true
}

func (api *API) registerRegionalSubnetworkOperation(operationType, targetLink, region string) (*orchestrator.Operation, error) {
	if api.opMgr == nil {
		return nil, errors.New("operation manager is unavailable")
	}
	return api.opMgr.RegisterDurable("compute#operation", operationType, targetLink, "", region)
}

func (api *API) saveMetadataPayload(payload []byte) error {
	if api.stateStore == nil {
		return nil
	}
	return api.stateStore.Save(computeStateEntry, json.RawMessage(payload))
}

func cloneSubnetwork(subnetwork *Subnetwork) *Subnetwork {
	if subnetwork == nil {
		return nil
	}
	clone := *subnetwork
	return &clone
}

func nextSubnetworkID(subnetworks map[string]*Subnetwork) uint64 {
	next := uint64(1)
	for _, subnetwork := range subnetworks {
		if subnetwork == nil {
			continue
		}
		id, err := strconv.ParseUint(subnetwork.ID, 10, 64)
		if err == nil && id >= next {
			next = id + 1
		}
	}
	return next
}

func networkSelfLink(project, name string) string {
	return fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/global/networks/%s", project, name)
}

func regionSelfLink(project, region string) string {
	return fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/regions/%s", project, region)
}

func subnetworkSelfLink(project, region, name string) string {
	return fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/regions/%s/subnetworks/%s",
		project, region, name)
}

func subnetworkKey(project, region, name string) string {
	return project + ":" + region + ":" + name
}

func subnetworkFingerprint(subnetwork *Subnetwork) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		subnetwork.Name,
		subnetwork.Description,
		subnetwork.IPCidrRange,
		subnetwork.Network,
		subnetwork.Region,
		"PRIVATE",
		"IPV4_ONLY",
	}, "\x00")))
	return base64.RawURLEncoding.EncodeToString(sum[:16])
}

type subnetworkPageToken struct {
	Version   int    `json:"v"`
	Project   string `json:"project"`
	Region    string `json:"region"`
	LastName  string `json:"lastName"`
	Integrity string `json:"integrity"`
}

func encodeSubnetworkPageToken(project, region, lastName string) string {
	cursor := subnetworkPageToken{
		Version: 1, Project: project, Region: region, LastName: lastName,
	}
	cursor.Integrity = subnetworkPageTokenIntegrity(cursor)
	payload, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(payload)
}

func decodeSubnetworkPageToken(project, region, token string) (string, error) {
	if token == "" {
		return "", nil
	}
	if len(token) > 4096 {
		return "", errors.New("page token is too large")
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", err
	}
	var cursor subnetworkPageToken
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cursor); err != nil {
		return "", err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return "", errors.New("page token has trailing data")
	}
	if cursor.Version != 1 || cursor.Project != project || cursor.Region != region ||
		!gceResourceName.MatchString(cursor.LastName) ||
		cursor.Integrity != subnetworkPageTokenIntegrity(cursor) {
		return "", errors.New("page token does not match collection")
	}
	return cursor.LastName, nil
}

func subnetworkPageTokenIntegrity(cursor subnetworkPageToken) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf(
		"minisky-compute-subnetwork-page-token\x00%d\x00%s\x00%s\x00%s",
		cursor.Version,
		cursor.Project,
		cursor.Region,
		cursor.LastName,
	)))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}
