// Command docs-truth renders compatibility and platform documentation from
// machine-readable repository evidence.
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"minisky/pkg/evidence"
	"minisky/pkg/registry"
	_ "minisky/pkg/shims"
)

const (
	serviceCatalogStart   = "<!-- BEGIN GENERATED SERVICE CATALOG -->"
	serviceCatalogEnd     = "<!-- END GENERATED SERVICE CATALOG -->"
	phaseSummaryStart     = "<!-- BEGIN GENERATED PHASE 18-25 SUMMARY -->"
	phaseSummaryEnd       = "<!-- END GENERATED PHASE 18-25 SUMMARY -->"
	readmeCountStart      = "<!-- BEGIN GENERATED REGISTRY COUNT -->"
	readmeCountEnd        = "<!-- END GENERATED REGISTRY COUNT -->"
	platformSummaryStart  = "<!-- BEGIN GENERATED PHASE 12 PLATFORM SUMMARY -->"
	platformSummaryEnd    = "<!-- END GENERATED PHASE 12 PLATFORM SUMMARY -->"
	memcacheSummaryStart  = "<!-- BEGIN GENERATED MEMCACHED SERVICE GATE -->"
	memcacheSummaryEnd    = "<!-- END GENERATED MEMCACHED SERVICE GATE -->"
	redisSummaryStart     = "<!-- BEGIN GENERATED REDIS SERVICE GATE -->"
	redisSummaryEnd       = "<!-- END GENERATED REDIS SERVICE GATE -->"
	emulatorBoundaryStart = "<!-- BEGIN GENERATED STORAGE PUBSUB BOUNDARY -->"
	emulatorBoundaryEnd   = "<!-- END GENERATED STORAGE PUBSUB BOUNDARY -->"
	certificationStart    = "<!-- BEGIN GENERATED STABLE SNAPSHOT CERTIFICATION -->"
	certificationEnd      = "<!-- END GENERATED STABLE SNAPSHOT CERTIFICATION -->"
	promotionSummaryStart = "<!-- BEGIN GENERATED PR22 PROMOTION EVIDENCE -->"
	promotionSummaryEnd   = "<!-- END GENERATED PR22 PROMOTION EVIDENCE -->"
)

func main() {
	check := flag.Bool("check", false, "fail if generated documentation is stale")
	root := flag.String("root", ".", "repository root")
	flag.Parse()

	if err := generate(filepath.Clean(*root), *check); err != nil {
		fmt.Fprintln(os.Stderr, "docs-truth:", err)
		os.Exit(1)
	}
}

func truth() ([]registry.Service, []evidence.PhaseService, error) {
	services, err := registry.Services()
	if err != nil {
		return nil, nil, err
	}
	inventory, err := evidence.Phase18To25()
	if err != nil {
		return nil, nil, err
	}
	return services, inventory, nil
}

func generate(root string, check bool) error {
	services, inventory, err := truth()
	if err != nil {
		return err
	}
	platformGates, err := evidence.PlatformGates()
	if err != nil {
		return err
	}
	platformSummary, err := renderPhase12PlatformSummary(platformGates)
	if err != nil {
		return err
	}
	serviceGates, err := evidence.ServiceGates()
	if err != nil {
		return err
	}
	memcacheGate, err := selectMemcacheServiceGate(services, serviceGates)
	if err != nil {
		return err
	}
	redisGate, err := selectRedisServiceGate(services, serviceGates)
	if err != nil {
		return err
	}
	cloudSQLGate, err := selectCloudSQLServiceGate(services, serviceGates)
	if err != nil {
		return err
	}
	emulatorGates, err := evidence.EmulatorBoundaryGates()
	if err != nil {
		return err
	}
	emulatorGate, err := selectStoragePubSubBoundaryGate(emulatorGates)
	if err != nil {
		return err
	}
	qualityGates, err := evidence.QualityGates()
	if err != nil {
		return err
	}
	windowsGate, err := selectWindowsStateMarkerGate(qualityGates)
	if err != nil {
		return err
	}
	certificationSummary, err := renderStableSnapshotCertification(
		cloudSQLGate,
		emulatorGate,
		windowsGate,
	)
	if err != nil {
		return err
	}
	promotionRevision, err := evidence.CurrentPromotionRevision()
	if err != nil {
		return err
	}
	batchGates, err := evidence.BatchGates()
	if err != nil {
		return err
	}
	promotionSummary, err := renderPromotionSummary(promotionRevision, batchGates, memcacheGate)
	if err != nil {
		return err
	}
	catalog, err := renderServiceCatalog(services, inventory)
	if err != nil {
		return err
	}
	summary, err := renderPhaseSummary(services, inventory)
	if err != nil {
		return err
	}

	docsPath := filepath.Join(root, "docs", "service-compatibility.md")
	docs, err := os.ReadFile(docsPath)
	if err != nil {
		return err
	}
	updatedDocs, err := replaceGeneratedSection(
		string(docs), serviceCatalogStart, serviceCatalogEnd, catalog,
	)
	if err != nil {
		return fmt.Errorf("%s: %w", docsPath, err)
	}
	updatedDocs, err = replaceGeneratedSection(
		updatedDocs, phaseSummaryStart, phaseSummaryEnd, summary,
	)
	if err != nil {
		return fmt.Errorf("%s: %w", docsPath, err)
	}
	updatedDocs, err = replaceMemcacheServiceGate(updatedDocs, memcacheGate)
	if err != nil {
		return fmt.Errorf("%s: %w", docsPath, err)
	}
	updatedDocs, err = replaceRedisServiceGate(updatedDocs, redisGate)
	if err != nil {
		return fmt.Errorf("%s: %w", docsPath, err)
	}
	updatedDocs, err = replaceStoragePubSubBoundary(updatedDocs, emulatorGate)
	if err != nil {
		return fmt.Errorf("%s: %w", docsPath, err)
	}
	updatedDocs, err = replaceGeneratedSection(
		updatedDocs, certificationStart, certificationEnd, certificationSummary,
	)
	if err != nil {
		return fmt.Errorf("%s: %w", docsPath, err)
	}
	updatedDocs, err = replaceGeneratedSection(
		updatedDocs, promotionSummaryStart, promotionSummaryEnd, promotionSummary,
	)
	if err != nil {
		return fmt.Errorf("%s: %w", docsPath, err)
	}
	if err := validateHandwrittenClaims(updatedDocs); err != nil {
		return fmt.Errorf("%s: %w", docsPath, err)
	}
	statePath := filepath.Join(root, "docs", "state-model.md")
	stateModel, err := os.ReadFile(statePath)
	if err != nil {
		return err
	}
	terraformPath := filepath.Join(root, "docs", "terraform-compatibility.md")
	terraformCompatibility, err := os.ReadFile(terraformPath)
	if err != nil {
		return err
	}
	updatedTerraform, err := replaceMemcacheServiceGate(string(terraformCompatibility), memcacheGate)
	if err != nil {
		return fmt.Errorf("%s: %w", terraformPath, err)
	}
	updatedTerraform, err = replaceRedisServiceGate(updatedTerraform, redisGate)
	if err != nil {
		return fmt.Errorf("%s: %w", terraformPath, err)
	}
	if err := validateMemcacheClaims(updatedDocs, string(stateModel), updatedTerraform); err != nil {
		return fmt.Errorf("Memcached documentation truth: %w", err)
	}
	if err := validateStoragePubSubClaims(updatedDocs, string(stateModel)); err != nil {
		return fmt.Errorf("Storage/PubSub documentation truth: %w", err)
	}
	if err := validateTerraformPromotionClaims(updatedTerraform, batchGates); err != nil {
		return fmt.Errorf("Terraform promotion documentation truth: %w", err)
	}

	readmePath := filepath.Join(root, "README.md")
	readme, err := os.ReadFile(readmePath)
	if err != nil {
		return err
	}
	count, err := renderRegistryCount(services)
	if err != nil {
		return err
	}
	updatedReadme, err := replaceGeneratedSection(
		string(readme), readmeCountStart, readmeCountEnd, count,
	)
	if err != nil {
		return fmt.Errorf("%s: %w", readmePath, err)
	}
	updatedReadme, err = replaceMemcacheServiceGate(updatedReadme, memcacheGate)
	if err != nil {
		return fmt.Errorf("%s: %w", readmePath, err)
	}
	updatedReadme, err = replaceRedisServiceGate(updatedReadme, redisGate)
	if err != nil {
		return fmt.Errorf("%s: %w", readmePath, err)
	}
	updatedReadme, err = replaceStoragePubSubBoundary(updatedReadme, emulatorGate)
	if err != nil {
		return fmt.Errorf("%s: %w", readmePath, err)
	}
	updatedReadme, err = replaceGeneratedSection(
		updatedReadme, certificationStart, certificationEnd, certificationSummary,
	)
	if err != nil {
		return fmt.Errorf("%s: %w", readmePath, err)
	}
	updatedReadme, err = replaceGeneratedSection(
		updatedReadme, phaseSummaryStart, phaseSummaryEnd, summary,
	)
	if err != nil {
		return fmt.Errorf("%s: %w", readmePath, err)
	}
	updatedReadme, err = replaceGeneratedSection(
		updatedReadme, promotionSummaryStart, promotionSummaryEnd, promotionSummary,
	)
	if err != nil {
		return fmt.Errorf("%s: %w", readmePath, err)
	}
	updatedReadme, err = replaceGeneratedSection(
		updatedReadme, platformSummaryStart, platformSummaryEnd, platformSummary,
	)
	if err != nil {
		return fmt.Errorf("%s: %w", readmePath, err)
	}
	if err := validatePhase12Claims(updatedReadme, platformGates); err != nil {
		return fmt.Errorf("%s: %w", readmePath, err)
	}
	if err := validateRedisClaims(
		updatedReadme,
		updatedDocs,
		string(stateModel),
		updatedTerraform,
		redisGate,
	); err != nil {
		return fmt.Errorf("Redis documentation truth: %w", err)
	}
	if err := validateCloudSQLClaims(
		updatedReadme,
		updatedDocs,
		string(stateModel),
		updatedTerraform,
		cloudSQLGate,
	); err != nil {
		return fmt.Errorf("Cloud SQL documentation truth: %w", err)
	}
	if err := validatePromotionWorkflowClaims(
		updatedReadme,
		updatedDocs,
		updatedTerraform,
	); err != nil {
		return fmt.Errorf("promotion workflow documentation truth: %w", err)
	}
	if err := validateStableCertificationClaims(
		[]string{updatedReadme, updatedDocs, updatedTerraform},
		cloudSQLGate,
		emulatorGate,
		windowsGate,
	); err != nil {
		return fmt.Errorf("stable certification documentation truth: %w", err)
	}
	for _, path := range []string{
		filepath.Join(root, "docs", "minisky-roadmap-completion-plan.canvas.tsx"),
		filepath.Join(root, "docs", "adr", "0012-local-observability-and-request-replay.md"),
	} {
		document, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := validatePhase12Claims(string(document), platformGates); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		if strings.HasSuffix(path, ".canvas.tsx") {
			if err := validateRoadmapCertificationClaims(
				string(document),
				cloudSQLGate,
				windowsGate,
			); err != nil {
				return fmt.Errorf("%s: %w", path, err)
			}
			if err := validateRoadmapRedisClaims(string(document), redisGate); err != nil {
				return fmt.Errorf("%s: %w", path, err)
			}
		}
	}

	targets := []struct {
		path string
		data []byte
	}{
		{docsPath, []byte(updatedDocs)},
		{readmePath, []byte(updatedReadme)},
		{terraformPath, []byte(updatedTerraform)},
		{filepath.Join(root, "cmd", "docs-truth", "testdata", "service-catalog.golden.md"), []byte(catalog)},
	}
	var drift []string
	for _, target := range targets {
		if err := writeOrCheck(target.path, target.data, check); err != nil {
			if check && errors.Is(err, errDrift) {
				drift = append(drift, target.path)
				continue
			}
			return err
		}
	}
	if len(drift) > 0 {
		return fmt.Errorf("generated documentation drift: %s; run go run ./cmd/docs-truth", strings.Join(drift, ", "))
	}
	return nil
}

func selectMemcacheServiceGate(
	services []registry.Service,
	gates []evidence.ServiceGate,
) (evidence.ServiceGate, error) {
	var matches []evidence.ServiceGate
	for _, gate := range gates {
		if gate.Domain == "memcache.googleapis.com" {
			matches = append(matches, gate)
		}
	}
	if len(matches) != 1 {
		return evidence.ServiceGate{}, fmt.Errorf(
			"Memcached service gate count is %d, want exactly one",
			len(matches),
		)
	}
	gate := matches[0]
	registered := false
	for _, service := range services {
		if service.Domain == gate.Domain {
			registered = true
			break
		}
	}
	if !registered {
		return evidence.ServiceGate{}, fmt.Errorf(
			"Memcached service gate references unregistered domain %q",
			gate.Domain,
		)
	}
	if gate.ID == "" || gate.Phase != 15 || gate.ProviderVersion == "" ||
		gate.Script == "" || gate.MakeTarget == "" ||
		len(gate.Dimensions) == 0 {
		return evidence.ServiceGate{}, errors.New("Memcached service gate is missing required lifecycle metadata")
	}
	if err := validateMemcacheLocal(gate.EvidenceCheck); err != nil {
		return evidence.ServiceGate{}, err
	}
	if err := validateMemcacheCI(gate.CI); err != nil {
		return evidence.ServiceGate{}, err
	}
	return gate, nil
}

func selectRedisServiceGate(
	services []registry.Service,
	gates []evidence.ServiceGate,
) (evidence.ServiceGate, error) {
	var matches []evidence.ServiceGate
	for _, gate := range gates {
		if gate.ID == "phase15-redis" {
			matches = append(matches, gate)
		}
	}
	if len(matches) != 1 {
		return evidence.ServiceGate{}, fmt.Errorf(
			"Redis service gate count is %d, want exactly one",
			len(matches),
		)
	}
	gate := matches[0]
	registered := false
	for _, service := range services {
		if service.Domain == gate.Domain {
			registered = true
			break
		}
	}
	if !registered || gate.Domain != "redis.googleapis.com" {
		return evidence.ServiceGate{}, errors.New(
			"Redis service gate must reference the registered Redis domain",
		)
	}
	if gate.Phase != 15 || gate.ProviderVersion != "7.41.0" ||
		gate.Script != "scripts/redis-integration.sh" ||
		gate.MakeTarget != "test-redis-integration" ||
		len(gate.Dimensions) == 0 {
		return evidence.ServiceGate{}, errors.New("Redis service gate is missing required lifecycle metadata")
	}
	if err := evidence.ValidateEvidenceCheck(gate.EvidenceCheck); err != nil {
		return evidence.ServiceGate{}, fmt.Errorf("Redis local evidence: %w", err)
	}
	switch gate.Status {
	case evidence.EvidenceConfiguredUnverified:
		if gate.SourceCommit != "" || gate.SourceSHA256 != "" || gate.DiffSHA256 != "" {
			return evidence.ServiceGate{}, errors.New("configured Redis evidence must not include source provenance")
		}
	case evidence.EvidenceLocalPassedUncommitted:
		if gate.SourceCommit != "" || gate.SourceSHA256 == "" || gate.DiffSHA256 == "" {
			return evidence.ServiceGate{}, errors.New("uncommitted Redis pass requires stable source/diff provenance")
		}
	case evidence.EvidenceLocalPassed:
		if gate.SourceCommit == "" {
			return evidence.ServiceGate{}, errors.New("immutable Redis pass requires a source commit")
		}
	default:
		return evidence.ServiceGate{}, fmt.Errorf("unsupported Redis local evidence status %q", gate.Status)
	}
	if err := evidence.ValidateEvidenceCheck(gate.CI); err != nil {
		return evidence.ServiceGate{}, fmt.Errorf("Redis CI evidence: %w", err)
	}
	if gate.CI.Workflow != ".github/workflows/critical-integration.yml" ||
		gate.CI.Job != "redis-integration" {
		return evidence.ServiceGate{}, errors.New("Redis CI evidence does not identify its required job")
	}
	if gate.CI.Status == evidence.EvidenceConfiguredUnverified {
		if gate.CI.RunURL != "" || gate.CI.JobURL != "" || gate.CI.Commit != "" {
			return evidence.ServiceGate{}, errors.New("configured Redis CI evidence includes pass provenance")
		}
	} else if gate.CI.Status != evidence.EvidenceCIPassed ||
		gate.CI.RunURL == "" || gate.CI.JobURL == "" || gate.CI.Commit == "" {
		return evidence.ServiceGate{}, errors.New("Redis CI evidence status is unsupported or incomplete")
	}
	return gate, nil
}

func renderRedisServiceGate(gate evidence.ServiceGate) (string, error) {
	if _, err := selectRedisServiceGate(
		[]registry.Service{{Domain: "redis.googleapis.com"}},
		[]evidence.ServiceGate{gate},
	); err != nil {
		return "", err
	}
	var local string
	switch gate.Status {
	case evidence.EvidenceConfiguredUnverified:
		local = fmt.Sprintf(
			"Gate `%s` is `%s`: `%s` (`make %s`) is implemented, but no complete local pass is recorded.",
			gate.ID,
			gate.Status,
			gate.Script,
			gate.MakeTarget,
		)
	case evidence.EvidenceLocalPassedUncommitted:
		local = fmt.Sprintf(
			"Gate `%s` is `%s` at source SHA-256 `%s` and diff SHA-256 `%s` via `%s` (`make %s`).",
			gate.ID,
			gate.Status,
			gate.SourceSHA256,
			gate.DiffSHA256,
			gate.Script,
			gate.MakeTarget,
		)
	case evidence.EvidenceLocalPassed:
		local = fmt.Sprintf(
			"Gate `%s` is `%s` at immutable source commit `%s` via `%s` (`make %s`).",
			gate.ID,
			gate.Status,
			gate.SourceCommit,
			gate.Script,
			gate.MakeTarget,
		)
	}
	var ci string
	if gate.CI.Status == evidence.EvidenceConfiguredUnverified {
		ci = "CI is `configured-unverified`; no external run URL or commit is recorded."
	} else {
		ci = fmt.Sprintf(
			"CI is `ci-passed` in [the recorded run](%s) ([job](%s)) at commit `%s`.",
			gate.CI.RunURL,
			gate.CI.JobURL,
			gate.CI.Commit,
		)
	}
	dimensions := make([]string, len(gate.Dimensions))
	for index, dimension := range gate.Dimensions {
		dimensions[index] = "`" + dimension + "`"
	}
	return fmt.Sprintf(
		"**Generated Redis durability-gate truth:** %s Google provider `%s`.\n\n"+
			"Lifecycle dimensions (%d): %s.\n\n"+
			"%s Boundaries: no portable AOF export, HA/failover, TLS/IAM/VPC/PSC, Redis Cluster, "+
			"or hostile-daemon guarantee. Exact volume deletion remains a cooperative Docker daemon, "+
			"non-atomic re-inspect/delete boundary. A process or host crash after a Docker create "+
			"side effect but before resolved provenance is persisted can leave exact-labelled resources; "+
			"deterministic names and labels aid explicit cleanup, but restart neither adopts nor "+
			"automatically removes them.\n",
		local,
		gate.ProviderVersion,
		len(dimensions),
		strings.Join(dimensions, ", "),
		ci,
	), nil
}

func replaceRedisServiceGate(document string, gate evidence.ServiceGate) (string, error) {
	summary, err := renderRedisServiceGate(gate)
	if err != nil {
		return "", err
	}
	return replaceGeneratedSection(document, redisSummaryStart, redisSummaryEnd, summary)
}

func validateRedisClaims(
	readme string,
	serviceCompatibility string,
	stateModel string,
	terraformCompatibility string,
	gate evidence.ServiceGate,
) error {
	summary, err := renderRedisServiceGate(gate)
	if err != nil {
		return err
	}
	for name, document := range map[string]string{
		"README":                  readme,
		"service compatibility":   serviceCompatibility,
		"Terraform compatibility": terraformCompatibility,
	} {
		if strings.Count(document, redisSummaryStart) != 1 ||
			strings.Count(document, redisSummaryEnd) != 1 ||
			!strings.Contains(document, strings.TrimSpace(summary)) {
			return fmt.Errorf("%s does not contain exactly one current generated Redis gate", name)
		}
	}
	normalizedState := strings.ToLower(strings.Join(strings.Fields(stateModel), " "))
	for _, required := range []string{
		"redis backend metadata",
		"aof volume contents are not portable",
		"cooperative docker daemon",
		"before resolved provenance is persisted",
		"restart neither adopts nor automatically removes",
	} {
		if !strings.Contains(normalizedState, required) {
			return fmt.Errorf("state model is missing Redis boundary %q", required)
		}
	}
	return nil
}

func validateRoadmapRedisClaims(document string, gate evidence.ServiceGate) error {
	required := []string{
		"phase15-redis",
		string(gate.Status),
		"configured-unverified",
		"portable AOF",
		"cooperative Docker daemon",
		"pre-persistence crash",
	}
	for _, fragment := range required {
		if !strings.Contains(document, fragment) {
			return fmt.Errorf("roadmap canvas is missing Redis boundary %q", fragment)
		}
	}
	if gate.Status == evidence.EvidenceConfiguredUnverified &&
		(strings.Contains(document, "Redis phase15-redis is local-passed") ||
			strings.Contains(document, "Redis phase15-redis is ci-passed")) {
		return errors.New("roadmap canvas overstates configured Redis evidence")
	}
	return nil
}

func selectCloudSQLServiceGate(
	services []registry.Service,
	gates []evidence.ServiceGate,
) (evidence.ServiceGate, error) {
	var matches []evidence.ServiceGate
	for _, gate := range gates {
		if gate.ID == "cloudsql-restart-recovery" {
			matches = append(matches, gate)
		}
	}
	if len(matches) != 1 {
		return evidence.ServiceGate{}, fmt.Errorf(
			"Cloud SQL restart recovery gate count is %d, want exactly one",
			len(matches),
		)
	}
	gate := matches[0]
	registered := false
	for _, service := range services {
		if service.Domain == gate.Domain {
			registered = true
			break
		}
	}
	if !registered || gate.Domain != "sqladmin.googleapis.com" {
		return evidence.ServiceGate{}, errors.New("Cloud SQL recovery gate does not reference the registered SQL Admin domain")
	}
	if gate.Status != evidence.EvidenceLocalPassedUncommitted ||
		gate.SourceCommit != "" || gate.SourceSHA256 == "" || gate.DiffSHA256 == "" {
		return evidence.ServiceGate{}, errors.New("Cloud SQL recovery must use stable uncommitted source/diff provenance")
	}
	if gate.CI.Status != evidence.EvidenceCIPassed ||
		gate.CI.Workflow != ".github/workflows/critical-integration.yml" ||
		gate.CI.Job != "cloudsql-restart-integration" ||
		gate.CI.RunURL == "" || gate.CI.JobURL == "" || gate.CI.Commit == "" {
		return evidence.ServiceGate{}, errors.New("Cloud SQL recovery CI requires exact immutable job provenance")
	}
	return gate, nil
}

func selectWindowsStateMarkerGate(
	gates []evidence.QualityGate,
) (evidence.QualityGate, error) {
	var matches []evidence.QualityGate
	for _, gate := range gates {
		if gate.ID == "windows-state-markers" {
			matches = append(matches, gate)
		}
	}
	if len(matches) != 1 {
		return evidence.QualityGate{}, fmt.Errorf(
			"Windows state-marker gate count is %d, want exactly one",
			len(matches),
		)
	}
	gate := matches[0]
	if !stringSetEqual(gate.RequiredBy, []string{"quality"}) ||
		gate.LocalPrerequisites.Status != evidence.EvidenceLocalPassedUncommitted ||
		gate.LocalPrerequisites.SourceSHA256 == "" ||
		gate.LocalPrerequisites.DiffSHA256 == "" {
		return evidence.QualityGate{}, errors.New("Windows state-marker local prerequisites lack bounded stable-snapshot evidence")
	}
	if gate.NativeCI.Status != evidence.EvidenceCIPassed ||
		gate.NativeCI.Workflow != ".github/workflows/ci.yml" ||
		gate.NativeCI.Job != "windows-state-markers" ||
		gate.NativeCI.RunURL == "" || gate.NativeCI.JobURL == "" || gate.NativeCI.Commit == "" {
		return evidence.QualityGate{}, errors.New("Windows native state-marker evidence requires exact immutable job provenance")
	}
	if gate.AuthoritativeQuality.Status != evidence.EvidenceCIPassed ||
		gate.AuthoritativeQuality.Workflow != ".github/workflows/ci.yml" ||
		gate.AuthoritativeQuality.Job != "quality" ||
		gate.AuthoritativeQuality.RunURL != gate.NativeCI.RunURL ||
		gate.AuthoritativeQuality.JobURL == "" ||
		gate.AuthoritativeQuality.Commit != gate.NativeCI.Commit {
		return evidence.QualityGate{}, errors.New("authoritative quality evidence must match the native state-marker run and commit")
	}
	return gate, nil
}

func validateMemcacheLocal(check evidence.EvidenceCheck) error {
	if err := evidence.ValidateEvidenceCheck(check); err != nil {
		return fmt.Errorf("Memcached local evidence: %w", err)
	}
	switch check.Status {
	case evidence.EvidenceLocalPassed:
		if check.SourceCommit == "" {
			return errors.New("Memcached immutable local evidence requires a full source commit")
		}
	case evidence.EvidenceLocalPassedUncommitted:
		if check.SourceCommit != "" {
			return errors.New("Memcached uncommitted local evidence must not include a source commit")
		}
	default:
		return fmt.Errorf(
			"unsupported Memcached local evidence status %q; want local-passed-uncommitted or local-passed",
			check.Status,
		)
	}
	return nil
}

func validateMemcacheCI(check evidence.EvidenceCheck) error {
	if err := evidence.ValidateEvidenceCheck(check); err != nil {
		return fmt.Errorf("Memcached CI evidence: %w", err)
	}
	if check.Workflow == "" || check.Job == "" {
		return errors.New("Memcached CI evidence requires workflow and job")
	}
	switch check.Status {
	case evidence.EvidenceConfiguredUnverified:
		if check.RunURL != "" || check.JobURL != "" || check.Commit != "" {
			return errors.New("Memcached configured CI evidence must not include a run URL, job URL, or commit")
		}
	case evidence.EvidenceCIPassed:
		if check.RunURL == "" || check.JobURL == "" || check.Commit == "" {
			return errors.New("Memcached ci-passed evidence requires immutable run URL, job URL, and commit")
		}
	default:
		return fmt.Errorf(
			"unsupported Memcached CI evidence status %q; want configured-unverified or ci-passed",
			check.Status,
		)
	}
	return nil
}

func renderMemcacheServiceGate(gate evidence.ServiceGate) (string, error) {
	if err := validateMemcacheLocal(gate.EvidenceCheck); err != nil {
		return "", err
	}
	if err := validateMemcacheCI(gate.CI); err != nil {
		return "", err
	}
	dimensions := make([]string, len(gate.Dimensions))
	for index, dimension := range gate.Dimensions {
		dimensions[index] = "`" + dimension + "`"
	}
	var localStatement string
	switch gate.Status {
	case evidence.EvidenceLocalPassedUncommitted:
		localStatement = fmt.Sprintf(
			"The bounded lifecycle is `%s` in the current working tree with Google provider `%s`, "+
				"`make %s`, and `%s`. This locally passing working-tree gate is non-promotable "+
				"and has no immutable source revision evidence.",
			gate.Status,
			gate.ProviderVersion,
			gate.MakeTarget,
			gate.Script,
		)
	case evidence.EvidenceLocalPassed:
		localStatement = fmt.Sprintf(
			"The bounded lifecycle is `%s` at immutable source commit `%s` with Google provider `%s`, "+
				"`make %s`, and `%s`.",
			gate.Status,
			gate.SourceCommit,
			gate.ProviderVersion,
			gate.MakeTarget,
			gate.Script,
		)
	default:
		return "", fmt.Errorf("unsupported Memcached local evidence status %q", gate.Status)
	}
	var ciStatement string
	switch gate.CI.Status {
	case evidence.EvidenceConfiguredUnverified:
		ciStatement = fmt.Sprintf(
			"CI is `configured-unverified` in `%s` job `%s`; no external run URL or commit is recorded.",
			gate.CI.Workflow,
			gate.CI.Job,
		)
	case evidence.EvidenceCIPassed:
		ciStatement = fmt.Sprintf(
			"CI is `ci-passed` in [GitHub Actions run %s](%s) "+
				"([job](%s)) on commit `%s`.",
			path.Base(gate.CI.RunURL),
			gate.CI.RunURL,
			gate.CI.JobURL,
			gate.CI.Commit,
		)
	default:
		return "", fmt.Errorf("unsupported Memcached CI evidence status %q", gate.CI.Status)
	}
	return fmt.Sprintf(
		"**Generated Memcached service-gate truth:** %s\n\n"+
			"Lifecycle dimensions (%d): %s.\n\n"+
			"%s This evidence does not claim broad GCP parity or promote service fidelity.\n",
		localStatement,
		len(dimensions),
		strings.Join(dimensions, ", "),
		ciStatement,
	), nil
}

func replaceMemcacheServiceGate(document string, gate evidence.ServiceGate) (string, error) {
	summary, err := renderMemcacheServiceGate(gate)
	if err != nil {
		return "", err
	}
	return replaceGeneratedSection(
		document,
		memcacheSummaryStart,
		memcacheSummaryEnd,
		summary,
	)
}

func selectStoragePubSubBoundaryGate(
	gates []evidence.EmulatorBoundaryGate,
) (evidence.EmulatorBoundaryGate, error) {
	var matches []evidence.EmulatorBoundaryGate
	for _, gate := range gates {
		if gate.ID == "storage-persistence-pubsub-session" {
			matches = append(matches, gate)
		}
	}
	if len(matches) != 1 {
		return evidence.EmulatorBoundaryGate{}, fmt.Errorf(
			"Storage/PubSub boundary gate count is %d, want exactly one",
			len(matches),
		)
	}
	gate := matches[0]
	if len(gate.Domains) != 2 ||
		!stringSetEqual(gate.Domains, []string{"storage.googleapis.com", "pubsub.googleapis.com"}) {
		return evidence.EmulatorBoundaryGate{}, errors.New(
			"Storage/PubSub boundary gate must cover exactly Storage and Pub/Sub",
		)
	}
	if gate.Status != evidence.EvidenceLocalPassedUncommitted ||
		gate.SourceCommit != "" ||
		gate.SourceSHA256 == "" ||
		gate.DiffSHA256 == "" ||
		gate.CI.Status != evidence.EvidenceCIPassed ||
		gate.CI.RunURL == "" ||
		gate.CI.JobURL == "" ||
		gate.CI.Commit == "" {
		return evidence.EmulatorBoundaryGate{}, errors.New(
			"Storage/PubSub boundary requires uncommitted local evidence plus immutable CI provenance",
		)
	}
	return gate, nil
}

func renderStoragePubSubBoundary(gate evidence.EmulatorBoundaryGate) (string, error) {
	if _, err := selectStoragePubSubBoundaryGate([]evidence.EmulatorBoundaryGate{gate}); err != nil {
		return "", err
	}
	requiredTest := false
	for _, reference := range gate.References {
		for _, test := range reference.Tests {
			if test == "TestStoragePersistenceAndPubSubSessionBoundaries" {
				requiredTest = true
			}
		}
	}
	if !requiredTest {
		return "", errors.New("Storage/PubSub boundary gate is missing its live lifecycle test")
	}
	return fmt.Sprintf(
		"**Generated Storage/Pub/Sub boundary truth:** The `%s` working-tree gate runs "+
			"`make %s`, `%s`, and `TestStoragePersistenceAndPubSubSessionBoundaries`. "+
			"Stable snapshot source SHA-256: `%s`; diff SHA-256: `%s`. "+
			"CI is `%s` in [GitHub Actions run %s](%s) ([job](%s)) on exact PR #23 head commit `%s`; "+
			"the stable local fingerprints remain separate from immutable CI provenance.\n\n"+
			"The exact pinned public Pub/Sub image is acquired against the active daemon with an "+
			"isolated anonymous Docker configuration, then checked for immutable digest syntax, "+
			"`linux/amd64` platform execution, and advertised `--data-dir` capability. "+
			"Storage uses a profile-scoped runtime bind mount. Buckets and objects survive "+
			"exact-owned Storage emulator-container replacement. Pub/Sub resources and messages "+
			"last only for one official emulator session: MiniSky process crash/restart continuity "+
			"is supported only while the same exact-owned Pub/Sub container remains alive. "+
			"Replacing the Pub/Sub backend/container loses topics, subscriptions, and queued messages. "+
			"Graceful MiniSky shutdown tears down managed Docker resources and is not a Pub/Sub continuity path.\n\n"+
			"Storage and Pub/Sub runtime data remain outside metadata export/import. This gate does "+
			"not claim exactly-once delivery, portable data export, IAM, HA, security, or full GCP parity. "+
			"Its Docker cleanup evidence assumes cooperative, exclusive use of the managed resource names. "+
			"Docker volume deletion accepts only a mutable name, not a conditional immutable identity: "+
			"MiniSky revalidates exact ownership and identity immediately before deletion and fails closed, "+
			"but a foreign replacement in the final inspect-to-delete interval cannot be excluded atomically. "+
			"This is a bounded cleanup invariant, not a hostile-daemon security boundary. Public registry/network "+
			"access remains required; the global unowned image cache may retain an authorized pull; Pub/Sub remains "+
			"amd64/emulation/session-only. Five unrelated local volumes and a pre-existing lock observed during "+
			"certification are not product evidence.\n",
		gate.Status,
		gate.MakeTarget,
		gate.Script,
		gate.SourceSHA256,
		gate.DiffSHA256,
		gate.CI.Status,
		path.Base(gate.CI.RunURL),
		gate.CI.RunURL,
		gate.CI.JobURL,
		gate.CI.Commit,
	), nil
}

func renderStableSnapshotCertification(
	cloudSQL evidence.ServiceGate,
	storagePubSub evidence.EmulatorBoundaryGate,
	windows evidence.QualityGate,
) (string, error) {
	sourceSHA := cloudSQL.SourceSHA256
	diffSHA := cloudSQL.DiffSHA256
	if sourceSHA == "" || diffSHA == "" ||
		storagePubSub.SourceSHA256 != sourceSHA ||
		storagePubSub.DiffSHA256 != diffSHA ||
		windows.LocalPrerequisites.SourceSHA256 != sourceSHA ||
		windows.LocalPrerequisites.DiffSHA256 != diffSHA {
		return "", errors.New("stable certification gates do not share exact source/diff fingerprints")
	}
	if cloudSQL.Status != evidence.EvidenceLocalPassedUncommitted ||
		storagePubSub.Status != evidence.EvidenceLocalPassedUncommitted ||
		cloudSQL.CI.Status != evidence.EvidenceCIPassed ||
		storagePubSub.CI.Status != evidence.EvidenceCIPassed ||
		windows.NativeCI.Status != evidence.EvidenceCIPassed ||
		windows.AuthoritativeQuality.Status != evidence.EvidenceCIPassed {
		return "", errors.New("stable certification statuses are inconsistent")
	}
	if cloudSQL.CI.Commit != storagePubSub.CI.Commit ||
		cloudSQL.CI.Commit != windows.NativeCI.Commit ||
		cloudSQL.CI.Commit != windows.AuthoritativeQuality.Commit {
		return "", errors.New("stable certification CI commits disagree")
	}
	return fmt.Sprintf(
		"**Generated stable-snapshot certification:** The stable local certification remains identified by "+
			"source SHA-256 `%s` and diff SHA-256 `%s`. PR #23 exact-head commit `%s` has immutable CI evidence; "+
			"historical PR #22 evidence remains separate.\n\n"+
			"- Cloud SQL restart recovery is `%s`: live `POSTGRES_16` row survival passed through "+
			"same-container restart and volume-only recovery, followed by exact cleanup; the bounded "+
			"Terraform apply/no-drift/destroy lifecycle also passed. CI is `%s` in [critical run %s](%s) "+
			"([Cloud SQL job](%s)).\n"+
			"- Storage/Pub/Sub is `%s`: anonymous acquisition and immutable digest/platform/capability "+
			"checks passed before Storage replacement persistence, Pub/Sub session-loss boundaries, and exact cleanup. "+
			"CI is `%s` in the same [critical run %s](%s) ([Storage/Pub/Sub job](%s)).\n"+
			"- Native `windows-state-markers` is `%s` in [general CI run %s](%s) ([native job](%s)); "+
			"the authoritative `quality` aggregate is also `%s` ([quality job](%s)) in that exact run.\n\n"+
			"These exact-head PR #23 passes verify only the three listed gates and their documented boundaries. "+
			"PR #22 URLs apply only to their exact historical commit.\n",
		sourceSHA,
		diffSHA,
		cloudSQL.CI.Commit,
		cloudSQL.Status,
		cloudSQL.CI.Status,
		path.Base(cloudSQL.CI.RunURL),
		cloudSQL.CI.RunURL,
		cloudSQL.CI.JobURL,
		storagePubSub.Status,
		storagePubSub.CI.Status,
		path.Base(storagePubSub.CI.RunURL),
		storagePubSub.CI.RunURL,
		storagePubSub.CI.JobURL,
		windows.NativeCI.Status,
		path.Base(windows.NativeCI.RunURL),
		windows.NativeCI.RunURL,
		windows.NativeCI.JobURL,
		windows.AuthoritativeQuality.Status,
		windows.AuthoritativeQuality.JobURL,
	), nil
}

func replaceStoragePubSubBoundary(
	document string,
	gate evidence.EmulatorBoundaryGate,
) (string, error) {
	summary, err := renderStoragePubSubBoundary(gate)
	if err != nil {
		return "", err
	}
	return replaceGeneratedSection(
		document,
		emulatorBoundaryStart,
		emulatorBoundaryEnd,
		summary,
	)
}

func renderPromotionSummary(
	revision evidence.PromotionRevision,
	gates []evidence.BatchGate,
	memcache evidence.ServiceGate,
) (string, error) {
	if revision.Commit == "" || revision.GeneralCI.Status != evidence.EvidenceCIPassed ||
		revision.CriticalReliability.Status != evidence.EvidenceCIPassed {
		return "", errors.New("promotion revision lacks exact general and critical CI passes")
	}
	if memcache.CI.Status != evidence.EvidenceCIPassed ||
		memcache.CI.Commit != revision.Commit ||
		memcache.CI.JobURL == "" {
		return "", errors.New("promotion revision lacks the matching Memcached CI pass")
	}
	promotionRun := ""
	var terraformJobs []string
	var sdkJobs []string
	addJob := func(label string, check evidence.EvidenceCheck, destination *[]string) error {
		if check.Status != evidence.EvidenceCIPassed ||
			check.Commit != revision.Commit ||
			check.RunURL == "" ||
			check.JobURL == "" {
			return fmt.Errorf("%s is not a job-linked CI pass on %s", label, revision.Commit)
		}
		if promotionRun == "" {
			promotionRun = check.RunURL
		} else if promotionRun != check.RunURL {
			return fmt.Errorf("%s belongs to a different promotion run", label)
		}
		*destination = append(*destination, fmt.Sprintf("[%s job](%s)", label, check.JobURL))
		return nil
	}
	for _, gate := range gates {
		for _, check := range gate.TerraformChecks {
			if err := addJob(check.MatrixID, check.CI, &terraformJobs); err != nil {
				return "", err
			}
		}
		if err := addJob(gate.ID+" SDK", gate.CI, &sdkJobs); err != nil {
			return "", err
		}
		if gate.BackendCI.Status != "" {
			if err := addJob(gate.ID+" backend", gate.BackendCI, &sdkJobs); err != nil {
				return "", err
			}
		}
	}
	if len(terraformJobs) != 12 || len(sdkJobs) != 7 {
		return "", fmt.Errorf(
			"promotion revision has %d Terraform and %d SDK/backend job links, want 12 and 7",
			len(terraformJobs),
			len(sdkJobs),
		)
	}
	sort.Strings(terraformJobs)
	sort.Strings(sdkJobs)
	return fmt.Sprintf(
		"**Generated PR #22 promotion truth:** Exact source revision `%s` passed "+
			"[general CI run %s](%s) ([job](%s)), "+
			"[critical reliability run %s](%s) ([job](%s)), and "+
			"[the bounded promotion run %s](%s). The Memcached lifecycle passed in the "+
			"critical run ([job](%s)).\n\n"+
			"All 12 Terraform jobs passed: %s.\n\n"+
			"All seven SDK/backend jobs passed: %s.\n\n"+
			"These immutable records apply only to that source revision. The current working-tree "+
			"promotion workflow does not retain a duplicate full-quality job: authoritative quality "+
			"checks remain in the separate general CI workflow, while `promotion-assets` builds and "+
			"shares `ui/dist` for the integration jobs. PR #22's URLs do not verify those current "+
			"workflow changes. The uncommitted Storage/Pub/Sub boundary gate is not attributed to "+
			"these historical runs.\n",
		revision.Commit,
		path.Base(revision.GeneralCI.RunURL),
		revision.GeneralCI.RunURL,
		revision.GeneralCI.JobURL,
		path.Base(revision.CriticalReliability.RunURL),
		revision.CriticalReliability.RunURL,
		revision.CriticalReliability.JobURL,
		path.Base(promotionRun),
		promotionRun,
		memcache.CI.JobURL,
		strings.Join(terraformJobs, ", "),
		strings.Join(sdkJobs, ", "),
	), nil
}

func validateStoragePubSubClaims(serviceCompatibility, stateModel string) error {
	for name, document := range map[string]string{
		"service compatibility": serviceCompatibility,
		"state model":           stateModel,
	} {
		lower := strings.ToLower(document)
		for _, forbidden := range []string{
			"storage is unmounted",
			"storage runtime is unmounted",
			"pub/sub survives backend replacement",
			"pub/sub survives container replacement",
			"pubsub survives backend replacement",
			"pubsub survives container replacement",
		} {
			if strings.Contains(lower, forbidden) {
				return fmt.Errorf("%s contains false emulator claim %q", name, forbidden)
			}
		}
	}
	for _, required := range []string{
		"Storage uses a profile-scoped runtime bind mount",
		"Replacing the Pub/Sub backend/container loses topics, subscriptions, and queued messages",
		"Graceful MiniSky shutdown tears down managed Docker resources",
		"Storage and Pub/Sub runtime data remain outside metadata export/import",
	} {
		if !strings.Contains(serviceCompatibility, required) {
			return fmt.Errorf("service compatibility is missing %q", required)
		}
	}
	for _, required := range []string{
		"profile-scoped runtime bind mount",
		"same exact-owned Pub/Sub container remains alive",
		"loses topics, subscriptions, and queued messages",
		"outside metadata export/import",
	} {
		if !strings.Contains(stateModel, required) {
			return fmt.Errorf("state model is missing %q", required)
		}
	}
	return nil
}

func validateCloudSQLClaims(
	readme string,
	serviceCompatibility string,
	stateModel string,
	terraformCompatibility string,
	gate evidence.ServiceGate,
) error {
	documents := map[string]string{
		"README":                  readme,
		"service compatibility":   serviceCompatibility,
		"state model":             stateModel,
		"Terraform compatibility": terraformCompatibility,
	}
	for name, document := range documents {
		lower := strings.ToLower(strings.Join(strings.Fields(document), " "))
		for _, stale := range []string{
			"database files live only in containers",
			"database files remain container runtime data",
			"add profile-scoped named volumes for database data",
			"not sql data durability",
			"local post-review gate status is pending",
			"local post-review status remains pending",
			"pending local post-review verification",
			"cloud sql restart recovery is pending",
		} {
			if strings.Contains(lower, stale) {
				return fmt.Errorf("%s retains stale Cloud SQL claim %q", name, stale)
			}
		}
	}
	required := map[string][]string{
		"README": {
			"exact-owned named volume",
			"volume-only replacement",
			"protocol and authenticated readiness",
			"Cloud SQL restart recovery is `local-passed-uncommitted`",
			"source SHA-256 `" + gate.SourceSHA256 + "`",
			"diff SHA-256 `" + gate.DiffSHA256 + "`",
			"CI is `ci-passed`",
			gate.CI.RunURL,
			gate.CI.JobURL,
			gate.CI.Commit,
		},
		"service compatibility": {
			"exact-owned named volume",
			"same-container restart",
			"volume-only replacement",
			"Cloud SQL restart recovery is `local-passed-uncommitted`",
		},
		"state model": {
			"immutable image and volume identities",
			"same-container restart",
			"volume-only replacement",
			"`POSTGRES_16`, `POSTGRES_17`, and `POSTGRES_18`",
			"protocol and authenticated readiness",
			"legacy metadata without complete runtime provenance fails closed",
			"outside metadata export/import",
			"final inspect-to-delete interval cannot be made atomic",
		},
		"Terraform compatibility": {
			"`cloudsql-restart-integration`",
			"`local-passed-uncommitted`",
			"source SHA-256 `" + gate.SourceSHA256 + "`",
			"diff SHA-256 `" + gate.DiffSHA256 + "`",
			"CI is `ci-passed`",
			gate.CI.RunURL,
			gate.CI.JobURL,
			gate.CI.Commit,
		},
	}
	for name, phrases := range required {
		document := strings.ToLower(strings.Join(strings.Fields(documents[name]), " "))
		for _, phrase := range phrases {
			if !strings.Contains(document, strings.ToLower(phrase)) {
				return fmt.Errorf("%s is missing Cloud SQL claim %q", name, phrase)
			}
		}
	}
	return nil
}

func validatePromotionWorkflowClaims(documents ...string) error {
	for _, document := range documents {
		normalized := strings.Join(strings.Fields(document), " ")
		lower := strings.ToLower(normalized)
		for _, stale := range []string{
			"while retaining the quality",
			"promotion workflow while retaining the quality",
			"workflow exclusively owns seven sdk/backend gates. it replaces 20 misleading manual shadows from the general ci workflow while retaining the quality",
		} {
			if strings.Contains(lower, stale) {
				return fmt.Errorf("documentation conflates historical and current promotion behavior: %q", stale)
			}
		}
	}
	joined := strings.Join(documents, "\n")
	for _, required := range []string{
		"current working-tree promotion workflow does not retain a duplicate full-quality job",
		"authoritative quality checks remain in the separate general CI workflow",
		"`promotion-assets` builds and shares `ui/dist`",
		"PR #22's URLs do not verify those current workflow changes",
	} {
		if !strings.Contains(joined, required) {
			return fmt.Errorf("documentation is missing current promotion workflow claim %q", required)
		}
	}
	return nil
}

func validateStableCertificationClaims(
	documents []string,
	cloudSQL evidence.ServiceGate,
	storagePubSub evidence.EmulatorBoundaryGate,
	windows evidence.QualityGate,
) error {
	joined := strings.Join(documents, "\n")
	for _, required := range []string{
		"source SHA-256 `" + cloudSQL.SourceSHA256 + "`",
		"diff SHA-256 `" + cloudSQL.DiffSHA256 + "`",
		"Cloud SQL restart recovery is `local-passed-uncommitted`",
		"Storage/Pub/Sub is `local-passed-uncommitted`",
		"PR #23 exact-head commit `" + cloudSQL.CI.Commit + "`",
		cloudSQL.CI.RunURL,
		cloudSQL.CI.JobURL,
		storagePubSub.CI.JobURL,
		"Native `windows-state-markers` is `ci-passed`",
		windows.NativeCI.RunURL,
		windows.NativeCI.JobURL,
		"authoritative `quality` aggregate is also `ci-passed`",
		windows.AuthoritativeQuality.JobURL,
		"PR #22 URLs apply only to their exact historical commit",
	} {
		if !strings.Contains(joined, required) {
			return fmt.Errorf("stable certification documentation is missing %q", required)
		}
	}
	lower := strings.ToLower(joined)
	for _, forbidden := range []string{
		"native `windows-state-markers` is `configured-unverified`",
		"windows-state-markers` is `local-passed",
		"cloud sql restart recovery is pending",
		"pr #22 urls verify pr #23",
	} {
		if strings.Contains(lower, forbidden) {
			return fmt.Errorf("stable certification documentation conflates status with %q", forbidden)
		}
	}
	if cloudSQL.SourceSHA256 != storagePubSub.SourceSHA256 ||
		cloudSQL.DiffSHA256 != storagePubSub.DiffSHA256 ||
		cloudSQL.SourceSHA256 != windows.LocalPrerequisites.SourceSHA256 ||
		cloudSQL.DiffSHA256 != windows.LocalPrerequisites.DiffSHA256 ||
		cloudSQL.CI.Commit != storagePubSub.CI.Commit ||
		cloudSQL.CI.Commit != windows.NativeCI.Commit ||
		cloudSQL.CI.Commit != windows.AuthoritativeQuality.Commit {
		return errors.New("stable certification machine evidence fingerprints disagree")
	}
	return nil
}

func validateRoadmapCertificationClaims(
	document string,
	cloudSQL evidence.ServiceGate,
	windows evidence.QualityGate,
) error {
	normalized := strings.Join(strings.Fields(document), " ")
	for _, required := range []string{
		cloudSQL.SourceSHA256,
		cloudSQL.DiffSHA256,
		"<Code>local-passed-uncommitted</Code> Cloud SQL recovery",
		"Storage/Pub/Sub boundary gate remains <Code>local-passed-uncommitted</Code>",
		"<Code>windows-state-markers</Code> is <Code>ci-passed</Code>",
		"authoritative <Code>quality</Code> aggregate also passed",
		cloudSQL.CI.Commit,
		cloudSQL.CI.RunURL,
		windows.NativeCI.RunURL,
		"PR #22 remains historical evidence",
	} {
		if !strings.Contains(normalized, required) {
			return fmt.Errorf("roadmap is missing stable certification claim %q", required)
		}
	}
	for _, forbidden := range []string{
		"Storage/Pub/Sub boundary remains uncommitted local evidence only",
		"windows-state-markers</Code> is <Code>local-passed",
		"windows-state-markers</Code> remains <Code>configured-unverified",
	} {
		if strings.Contains(normalized, forbidden) {
			return fmt.Errorf("roadmap retains stale certification claim %q", forbidden)
		}
	}
	return nil
}

func validateTerraformPromotionClaims(
	document string,
	gates []evidence.BatchGate,
) error {
	var matches []evidence.TerraformCheck
	for _, gate := range gates {
		for _, check := range gate.TerraformChecks {
			if check.Domain == "binaryauthorization.googleapis.com" {
				matches = append(matches, check)
			}
		}
	}
	if len(matches) != 1 {
		return fmt.Errorf(
			"Binary Authorization Terraform check count is %d, want exactly one",
			len(matches),
		)
	}
	check := matches[0]
	if check.CI.Status != evidence.EvidenceCIPassed ||
		check.CI.JobURL == "" ||
		check.CI.Commit == "" {
		return errors.New("Binary Authorization Terraform evidence is not an immutable CI pass")
	}
	normalized := strings.Join(strings.Fields(document), " ")
	required := fmt.Sprintf(
		"Binary Authorization Terraform leg is `ci-passed` in "+
			"[binary-authorization job](%s) on exact source revision `%s`",
		check.CI.JobURL,
		check.CI.Commit,
	)
	if !strings.Contains(normalized, required) {
		return errors.New("Terraform compatibility does not match Binary Authorization CI evidence")
	}
	lower := strings.ToLower(normalized)
	for _, stale := range []string{
		"gate is recorded as `local-passed` only",
		"there is no claimed ci run url or commit",
	} {
		if strings.Contains(lower, stale) {
			return fmt.Errorf("Terraform compatibility retains stale Binary Authorization claim %q", stale)
		}
	}
	return nil
}

func renderRegistryCount(services []registry.Service) (string, error) {
	counts := map[registry.SupportStatus]int{
		registry.SupportImplemented:  0,
		registry.SupportExperimental: 0,
		registry.SupportDeferred:     0,
	}
	for _, service := range services {
		if _, known := counts[service.Support]; !known {
			return "", fmt.Errorf("service %s has unsupported status %q", service.Domain, service.Support)
		}
		counts[service.Support]++
	}
	return fmt.Sprintf(
		"- **🚀 %d Registry-Verified Domains**: %d implemented, %d experimental, "+
			"and %d deferred.\n"+
			"  The exact counts and generated compatibility rows come from\n"+
			"  `registry.Services()`. Phase 18–25 inventory entries remain\n"+
			"  experimental and default-off. See [Service Compatibility](docs/service-compatibility.md).\n",
		len(services),
		counts[registry.SupportImplemented],
		counts[registry.SupportExperimental],
		counts[registry.SupportDeferred],
	), nil
}

func validateHandwrittenClaims(document string) error {
	if strings.Contains(document, "Terraform provider evidence remains absent") {
		return errors.New("stale batch-wide Terraform absence claim; use generated per-domain evidence")
	}
	return nil
}

func validateMemcacheClaims(serviceCompatibility, stateModel, terraformCompatibility string) error {
	start := strings.Index(serviceCompatibility, serviceCatalogStart)
	end := strings.Index(serviceCompatibility, serviceCatalogEnd)
	if start < 0 || end <= start {
		return errors.New("generated service catalog markers are missing or out of order")
	}
	catalog := serviceCompatibility[start:end]
	if count := strings.Count(catalog, "`memcache.googleapis.com`"); count != 1 {
		return fmt.Errorf("generated service catalog contains Memcached %d times, want exactly once", count)
	}
	const memcacheCatalogPrefix = "| `memcache.googleapis.com` | standard | hybrid | implemented | standard | No |"
	if !strings.Contains(catalog, memcacheCatalogPrefix) {
		return errors.New("generated service catalog does not classify Memcached as implemented standard/hybrid and default-on")
	}
	for name, document := range map[string]string{
		"service compatibility": serviceCompatibility,
		"state model":           stateModel,
	} {
		for _, line := range strings.Split(document, "\n") {
			lower := strings.ToLower(line)
			if !strings.Contains(lower, "memcache") {
				continue
			}
			if strings.Contains(lower, "501") || strings.Contains(lower, "deferred") ||
				strings.Contains(lower, "not implemented") {
				return fmt.Errorf("%s retains contradictory Memcached claim %q", name, strings.TrimSpace(line))
			}
		}
	}
	for _, required := range []string{
		"Memcached metadata is profile-persisted",
		"owned Memcached containers",
	} {
		if !strings.Contains(stateModel, required) {
			return fmt.Errorf("state model is missing %q", required)
		}
	}
	normalizedTerraform := strings.Join(strings.Fields(terraformCompatibility), " ")
	for _, required := range []string{
		"`memcache_custom_endpoint`",
		"`google_memcache_instance`",
		"`effective_labels` is computed by the",
		"provider from API `labels`",
	} {
		if !strings.Contains(normalizedTerraform, required) {
			return fmt.Errorf("Terraform compatibility is missing %q", required)
		}
	}
	for name, document := range map[string]string{
		"service compatibility":   serviceCompatibility,
		"Terraform compatibility": terraformCompatibility,
	} {
		if strings.Count(document, memcacheSummaryStart) != 1 ||
			strings.Count(document, memcacheSummaryEnd) != 1 {
			return fmt.Errorf("%s must contain exactly one generated Memcached service-gate section", name)
		}
	}
	for _, line := range strings.Split(terraformCompatibility, "\n") {
		lower := strings.ToLower(line)
		if strings.Contains(lower, "memcache") &&
			(strings.Contains(lower, "configured but unverified") ||
				strings.Contains(lower, "not recorded as passing")) {
			return fmt.Errorf("Terraform compatibility retains stale Memcached status: %q", strings.TrimSpace(line))
		}
	}
	return nil
}

func renderPhase12PlatformSummary(gates []evidence.PlatformGate) (string, error) {
	localPassed := 0
	localTotal := 0
	var ciCheck *evidence.EvidenceCheck
	seen := make(map[string]bool, len(gates))
	for _, gate := range gates {
		if gate.Phase != 12 {
			return "", fmt.Errorf("platform gate %s belongs to phase %d, want 12", gate.ID, gate.Phase)
		}
		if seen[gate.ID] {
			return "", fmt.Errorf("duplicate platform gate %q", gate.ID)
		}
		seen[gate.ID] = true
		if err := evidence.ValidateEvidenceCheck(gate.Check); err != nil {
			return "", fmt.Errorf("%s: %w", gate.ID, err)
		}
		if gate.ID == "phase12-ci" {
			if ciCheck != nil {
				return "", errors.New("duplicate phase12-ci platform gate")
			}
			check := gate.Check
			ciCheck = &check
		} else {
			localTotal++
		}
		switch gate.Check.Status {
		case evidence.EvidenceLocalPassed:
			localPassed++
		}
	}
	if ciCheck == nil {
		return "", errors.New("missing phase12-ci platform gate")
	}
	var ciStatement string
	switch ciCheck.Status {
	case evidence.EvidenceConfiguredUnverified:
		ciStatement = "Required pull-request/main CI and optional manual execution are configured, but no external Phase 12 pass is recorded."
	case evidence.EvidenceCIPassed:
		ciStatement = fmt.Sprintf(
			"Required Phase 12 CI passed in [GitHub Actions run %s](%s) on commit `%s`.",
			path.Base(ciCheck.RunURL), ciCheck.RunURL, ciCheck.Commit,
		)
	case evidence.EvidenceOptionalUnverified:
		ciStatement = "Phase 12 CI is optional and externally unverified."
	case evidence.EvidenceLocalPassed:
		ciStatement = "The Phase 12 CI check is recorded only as local-passed; no external CI pass is recorded."
	case evidence.EvidenceNotApplicable:
		ciStatement = "Phase 12 CI is marked not-applicable by machine evidence."
	case evidence.EvidenceAbsent:
		ciStatement = "Phase 12 CI evidence is absent."
	default:
		return "", fmt.Errorf("unsupported Phase 12 CI status %q", ciCheck.Status)
	}
	return fmt.Sprintf(
		"**Generated Phase 12 platform truth:** Local-only gates passed: %d/%d.\n\n"+
			"The bounded platform diagnostics slice covers bounded W3C propagation, sanitized structured access logs, "+
			"low-cardinality Prometheus metrics, bounded sanitized OTLP export inspection, exporter degradation without "+
			"changing API responses, bounded replay responses, and graceful shutdown. Replay provides project-keyed "+
			"lookup scoping, not cross-project authorization. %s\n\n"+
			"This platform diagnostics layer is separate from the experimental Phase 21–22 service domains. A persistent "+
			"trace backend, remote diagnostics listener, Cloud Logging parity, and RBAC replay isolation remain deferred.\n",
		localPassed, localTotal, ciStatement,
	), nil
}

var (
	phase12CIPassedPattern = regexp.MustCompile(
		`(?is)phase\s*12.{0,160}(?:ci[- ]verified|ci\s+(?:has\s+)?passed)|(?:ci[- ]verified|ci\s+(?:has\s+)?passed).{0,160}phase\s*12`,
	)
	phase12CIUnverifiedPattern = regexp.MustCompile(
		`(?is)phase\s*12.{0,160}(?:no\s+external.{0,20}pass.{0,20}recorded|externally\s+unverified|configured-unverified)|(?:no\s+external.{0,20}pass.{0,20}recorded|externally\s+unverified|configured-unverified).{0,160}phase\s*12`,
	)
)

func validatePhase12Claims(document string, gates []evidence.PlatformGate) error {
	var ciCheck *evidence.EvidenceCheck
	for _, gate := range gates {
		if gate.ID == "phase12-ci" {
			if ciCheck != nil {
				return errors.New("duplicate phase12-ci platform gate")
			}
			check := gate.Check
			ciCheck = &check
		}
	}
	if ciCheck == nil {
		return errors.New("missing phase12-ci platform gate")
	}
	if err := evidence.ValidateEvidenceCheck(*ciCheck); err != nil {
		return fmt.Errorf("phase12-ci: %w", err)
	}
	if ciCheck.Status != evidence.EvidenceCIPassed && phase12CIPassedPattern.MatchString(document) {
		return errors.New("Phase 12 cannot be described as CI-verified without ci-passed machine evidence")
	}
	if ciCheck.Status == evidence.EvidenceCIPassed && phase12CIUnverifiedPattern.MatchString(document) {
		return errors.New("Phase 12 cannot be described as externally unverified with ci-passed machine evidence")
	}
	return nil
}

func renderServiceCatalog(
	services []registry.Service,
	inventory []evidence.PhaseService,
) (string, error) {
	services = append([]registry.Service(nil), services...)
	inventory = append([]evidence.PhaseService(nil), inventory...)
	sort.Slice(services, func(i, j int) bool { return services[i].Domain < services[j].Domain })
	sort.Slice(inventory, func(i, j int) bool { return inventory[i].Domain < inventory[j].Domain })

	evidenceByDomain := make(map[string]evidence.PhaseService, len(inventory))
	for _, entry := range inventory {
		if _, duplicate := evidenceByDomain[entry.Domain]; duplicate {
			return "", fmt.Errorf("duplicate Phase 18-25 evidence for %s", entry.Domain)
		}
		evidenceByDomain[entry.Domain] = entry
	}

	var output strings.Builder
	output.WriteString("| Domain | Registry classification | Persistence | Support | Fidelity | Default-off | Method note / evidence tests | Terraform claim |\n")
	output.WriteString("|--------|-------------------------|-------------|---------|----------|-------------|------------------------------|-----------------|\n")
	for _, service := range services {
		entry, hasEvidence := evidenceByDomain[service.Domain]
		if service.Support == registry.SupportExperimental && !hasEvidence {
			return "", fmt.Errorf("experimental service %s has no Phase 18-25 evidence entry", service.Domain)
		}
		if hasEvidence {
			if service.Support != registry.SupportExperimental {
				return "", fmt.Errorf("%s has Phase 18-25 evidence but registry support is %s", service.Domain, service.Support)
			}
			if entry.Persistence != string(service.Persistence) {
				return "", fmt.Errorf(
					"%s evidence persistence %s differs from registry %s",
					service.Domain, entry.Persistence, service.Persistence,
				)
			}
			delete(evidenceByDomain, service.Domain)
		}

		fidelity := string(service.Fidelity)
		if fidelity == "" {
			fidelity = "— (not promoted)"
		}
		classification := string(service.Fidelity)
		if classification == "" {
			classification = string(service.Support)
		}
		defaultOff := "No"
		note := registryNote(service)
		terraformClaim := "Not inventoried"
		if hasEvidence {
			defaultOff = "Yes"
			note = fmt.Sprintf(
				"%s<br>Package tests only: %s",
				entry.MethodNote,
				formatTests(entry.Package, entry.Tests),
			)
			terraformClaim = "No"
			if entry.TerraformClaim {
				terraformClaim = "Yes"
			}
		}
		fmt.Fprintf(
			&output,
			"| `%s` | %s | %s | %s | %s | %s | %s | %s |\n",
			service.Domain,
			classification,
			service.Persistence,
			service.Support,
			fidelity,
			defaultOff,
			escapeCell(note),
			terraformClaim,
		)
	}
	if len(evidenceByDomain) > 0 {
		var unknown []string
		for domain := range evidenceByDomain {
			unknown = append(unknown, domain)
		}
		sort.Strings(unknown)
		return "", fmt.Errorf("evidence entries missing from registry: %s", strings.Join(unknown, ", "))
	}
	return output.String(), nil
}

func renderPhaseSummary(
	services []registry.Service,
	inventory []evidence.PhaseService,
) (string, error) {
	gates, err := evidence.BatchGates()
	if err != nil {
		return "", err
	}
	experimental := 0
	for _, service := range services {
		if service.Support == registry.SupportExperimental {
			experimental++
		}
	}
	if experimental != len(inventory) {
		return "", fmt.Errorf(
			"experimental registry count %d differs from Phase 18-25 inventory count %d",
			experimental, len(inventory),
		)
	}
	terraformClaims := 0
	persistence := make(map[string]int)
	for _, entry := range inventory {
		persistence[entry.Persistence]++
		if entry.TerraformClaim {
			terraformClaims++
		}
	}
	var categories []string
	for category, count := range persistence {
		categories = append(categories, fmt.Sprintf("%s=%d", category, count))
	}
	sort.Strings(categories)
	terraformClaimNoun := "claims"
	if terraformClaims == 1 {
		terraformClaimNoun = "claim"
	}

	packageLocal := 0
	strictIAMLocal := 0
	generatedLocal := 0
	generatedConfigured := 0
	restartLocal := 0
	cleanupLocal := 0
	ciPassed := 0
	ciConfigured := 0
	backendCITotal := 0
	backendCIPassed := 0
	backendCIConfigured := 0
	terraformChecks := 0
	terraformCIPassed := 0
	terraformCIConfigured := 0
	admissionReplayTotal := 0
	admissionReplayLocal := 0
	for _, gate := range gates {
		terraformChecks += len(gate.TerraformChecks)
		for _, check := range gate.TerraformChecks {
			if check.CI.Status == evidence.EvidenceCIPassed {
				terraformCIPassed++
			}
			if check.CI.Status == evidence.EvidenceConfiguredUnverified {
				terraformCIConfigured++
			}
		}
		if gate.PackageUnit.Status == evidence.EvidenceLocalPassed {
			packageLocal++
		}
		if gate.StrictIAM.Status == evidence.EvidenceLocalPassed {
			strictIAMLocal++
		}
		if gate.GeneratedClientLifecycle.Status == evidence.EvidenceLocalPassed {
			generatedLocal++
		}
		if gate.GeneratedClientLifecycle.Status == evidence.EvidenceConfiguredUnverified {
			generatedConfigured++
		}
		if gate.DaemonRestart.Status == evidence.EvidenceLocalPassed {
			restartLocal++
		}
		if gate.Cleanup.Status == evidence.EvidenceLocalPassed {
			cleanupLocal++
		}
		if gate.CI.Status == evidence.EvidenceCIPassed {
			ciPassed++
		}
		if gate.CI.Status == evidence.EvidenceConfiguredUnverified {
			ciConfigured++
		}
		if gate.BackendCI.Status != "" {
			backendCITotal++
		}
		if gate.BackendCI.Status == evidence.EvidenceCIPassed {
			backendCIPassed++
		}
		if gate.BackendCI.Status == evidence.EvidenceConfiguredUnverified {
			backendCIConfigured++
		}
		if gate.AdmissionReplay.Status != "" {
			admissionReplayTotal++
		}
		if gate.AdmissionReplay.Status == evidence.EvidenceLocalPassed {
			admissionReplayLocal++
		}
	}
	return fmt.Sprintf(
		"**Generated truth:** %d experimental; %d default-off; %d Terraform %s. "+
			"Persistence inventory: %s.\n\n"+
			"Machine-readable promotion matrix: %d batch gates and %d per-domain Terraform checks. Package-unit gates passed locally: %d/%d; "+
			"strict-IAM gates passed locally: %d/%d. Generated-client lifecycle gates passed locally: %d/%d; "+
			"configured but unverified: %d/%d. Restart gates passed locally: %d/%d; cleanup gates passed locally: %d/%d; "+
			"CI gates passed: %d/%d; configured but unverified: %d/%d. "+
			"Heavy backend CI gates passed: %d/%d; configured but unverified: %d/%d. "+
			"Terraform CI gates passed: %d/%d; configured but unverified: %d/%d. "+
			"Admission replay gates passed locally: %d/%d. "+
			"Package and IAM passes do not promote compatibility; "+
			"every inventoried service remains experimental until its required integration gates pass.\n",
		experimental,
		experimental,
		terraformClaims,
		terraformClaimNoun,
		strings.Join(categories, ", "),
		len(gates),
		terraformChecks,
		packageLocal,
		len(gates),
		strictIAMLocal,
		len(gates),
		generatedLocal,
		len(gates),
		generatedConfigured,
		len(gates),
		restartLocal,
		len(gates),
		cleanupLocal,
		len(gates),
		ciPassed,
		len(gates),
		ciConfigured,
		len(gates),
		backendCIPassed,
		backendCITotal,
		backendCIConfigured,
		backendCITotal,
		terraformCIPassed,
		terraformChecks,
		terraformCIConfigured,
		terraformChecks,
		admissionReplayLocal,
		admissionReplayTotal,
	), nil
}

func registryNote(service registry.Service) string {
	if service.SupportReason != "" {
		return service.SupportReason
	}
	if service.BackendContract != "" {
		return service.BackendContract
	}
	return "Registry metadata only; see the hand-written compatibility boundaries above"
}

func formatTests(packagePath string, tests []string) string {
	formatted := make([]string, len(tests))
	for index, test := range tests {
		formatted[index] = "`" + packagePath + "." + test + "`"
	}
	return strings.Join(formatted, "<br>")
}

func escapeCell(value string) string {
	return strings.ReplaceAll(value, "|", "\\|")
}

func stringSetEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	values := make(map[string]bool, len(left))
	for _, value := range left {
		values[value] = true
	}
	for _, value := range right {
		if !values[value] {
			return false
		}
	}
	return true
}

func replaceGeneratedSection(document, start, end, generated string) (string, error) {
	if strings.Count(document, start) != 1 || strings.Count(document, end) != 1 {
		return "", fmt.Errorf("expected exactly one %q and %q marker", start, end)
	}
	startIndex := strings.Index(document, start) + len(start)
	endIndex := strings.Index(document, end)
	if endIndex < startIndex {
		return "", fmt.Errorf("end marker %q precedes start marker", end)
	}
	generated = strings.TrimSuffix(generated, "\n")
	return document[:startIndex] + "\n" + generated + "\n" + document[endIndex:], nil
}

var errDrift = errors.New("generated file is stale")

func writeOrCheck(path string, want []byte, check bool) error {
	got, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if bytes.Equal(got, want) {
		return nil
	}
	if check {
		return fmt.Errorf("%w: %s", errDrift, path)
	}
	mode := os.FileMode(0o644)
	if info, statErr := os.Stat(path); statErr == nil {
		mode = info.Mode().Perm()
	}
	return os.WriteFile(path, want, mode)
}
