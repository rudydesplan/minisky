package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"strings"
	"time"

	aiplatform "google.golang.org/api/aiplatform/v1"
	dialogflow "google.golang.org/api/dialogflow/v3"
	documentai "google.golang.org/api/documentai/v1"
	"google.golang.org/api/googleapi"
	language "google.golang.org/api/language/v1"
	"google.golang.org/api/option"
	speech "google.golang.org/api/speech/v1"
	texttospeech "google.golang.org/api/texttospeech/v1"
	translate "google.golang.org/api/translate/v3"
	vision "google.golang.org/api/vision/v1"
)

const (
	optInEnv         = "MINISKY_PHASE23_EXPERIMENTAL_OPT_IN"
	googleAPIVersion = "v0.287.1"
	evidenceVersion  = 1
	maxEvidenceBytes = 16 << 10
)

var resourceIDPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

type config struct {
	mode, endpoint, project, location, evidencePath, sensitiveSentinel string
}

type evidence struct {
	Version                 int    `json:"version"`
	GoogleAPIVersion        string `json:"googleApiVersion"`
	Project                 string `json:"project"`
	Location                string `json:"location"`
	DocumentProcessor       string `json:"documentProcessor"`
	DialogflowAgent         string `json:"dialogflowAgent"`
	VertexIndex             string `json:"vertexIndex"`
	VertexModel             string `json:"vertexModel"`
	DocumentDeleteOperation string `json:"documentDeleteOperation,omitempty"`
	VertexIndexOperation    string `json:"vertexIndexOperation"`
	VertexModelOperation    string `json:"vertexModelOperation"`
	IdentityTranslation     bool   `json:"identityTranslation"`
	SemanticBoundaries      string `json:"semanticBoundaries"`
	PayloadBounds           string `json:"payloadBounds"`
	UnsupportedOptions      string `json:"unsupportedOptions"`
	ProjectIsolation        bool   `json:"projectIsolation"`
	LocalExtensions         string `json:"localExtensions"`
}

type clients struct {
	speech    *speech.Service
	tts       *texttospeech.Service
	translate *translate.Service
	vision    *vision.Service
	language  *language.Service
	document  *documentai.Service
	dialog    *dialogflow.Service
	vertex    *aiplatform.Service
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Phase 23 generated Go client smoke failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := configFromEnv()
	if err != nil {
		return err
	}
	if err := requireGoogleAPIVersion(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c, err := newClients(ctx, cfg.endpoint)
	if err != nil {
		return err
	}
	switch cfg.mode {
	case "gate":
		return proveDefaultGate(ctx, c, cfg)
	case "create":
		return createAndRecord(ctx, c, cfg)
	case "verify":
		return verifyRestart(ctx, c, cfg)
	case "delete":
		return deleteAndVerify(ctx, c, cfg)
	default:
		return fmt.Errorf("unsupported MINISKY_PHASE23_MODE %q", cfg.mode)
	}
}

func configFromEnv() (config, error) {
	cfg := config{
		mode:              env("MINISKY_PHASE23_MODE", "gate"),
		endpoint:          strings.TrimRight(strings.TrimSpace(os.Getenv("MINISKY_ENDPOINT")), "/"),
		project:           env("MINISKY_PROJECT_ID", "phase23-project"),
		location:          env("MINISKY_PHASE23_LOCATION", "us-central1"),
		evidencePath:      strings.TrimSpace(os.Getenv("MINISKY_PHASE23_EVIDENCE")),
		sensitiveSentinel: env("MINISKY_PHASE23_SENSITIVE_SENTINEL", "phase23-sensitive-probe"),
	}
	if err := validateLoopbackEndpoint(cfg.endpoint); err != nil {
		return config{}, err
	}
	for name, value := range map[string]string{"project": cfg.project, "location": cfg.location} {
		if !resourceIDPattern.MatchString(value) {
			return config{}, fmt.Errorf("%s %q must match %s", name, value, resourceIDPattern)
		}
	}
	if cfg.evidencePath == "" || !filepath.IsAbs(cfg.evidencePath) {
		return config{}, errors.New("MINISKY_PHASE23_EVIDENCE must be an absolute path")
	}
	if len(cfg.sensitiveSentinel) < 16 || len(cfg.sensitiveSentinel) > 128 ||
		strings.ContainsAny(cfg.sensitiveSentinel, "\r\n\x00") {
		return config{}, errors.New("MINISKY_PHASE23_SENSITIVE_SENTINEL must be 16-128 safe bytes")
	}
	if cfg.mode != "gate" && os.Getenv(optInEnv) != "1" {
		return config{}, fmt.Errorf("%s mode requires explicit %s=1", cfg.mode, optInEnv)
	}
	return cfg, nil
}

func validateLoopbackEndpoint(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "http" || parsed.Host == "" || parsed.User != nil ||
		parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Port() == "" {
		return errors.New("MINISKY_ENDPOINT must be an HTTP loopback origin with an explicit port and no path")
	}
	if host := parsed.Hostname(); !strings.EqualFold(host, "localhost") {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return errors.New("MINISKY_ENDPOINT must target localhost or a loopback IP")
		}
	}
	return nil
}

func requireGoogleAPIVersion() error {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return errors.New("Go build information is unavailable")
	}
	for _, dependency := range info.Deps {
		if dependency.Path == "google.golang.org/api" {
			if dependency.Version != googleAPIVersion {
				return fmt.Errorf("google.golang.org/api version=%q want=%q", dependency.Version, googleAPIVersion)
			}
			return nil
		}
	}
	return errors.New("google.golang.org/api dependency is absent from build information")
}

func newClients(ctx context.Context, endpoint string) (*clients, error) {
	opts := func(domain string) []option.ClientOption {
		return []option.ClientOption{
			option.WithoutAuthentication(),
			option.WithEndpoint(endpoint + "/_minisky/" + domain + "/"),
		}
	}
	var c clients
	var err error
	if c.speech, err = speech.NewService(ctx, opts("speech.googleapis.com")...); err != nil {
		return nil, fmt.Errorf("create Speech client: %w", err)
	}
	if c.tts, err = texttospeech.NewService(ctx, opts("texttospeech.googleapis.com")...); err != nil {
		return nil, fmt.Errorf("create Text-to-Speech client: %w", err)
	}
	if c.translate, err = translate.NewService(ctx, opts("translate.googleapis.com")...); err != nil {
		return nil, fmt.Errorf("create Translation client: %w", err)
	}
	if c.vision, err = vision.NewService(ctx, opts("vision.googleapis.com")...); err != nil {
		return nil, fmt.Errorf("create Vision client: %w", err)
	}
	if c.language, err = language.NewService(ctx, opts("language.googleapis.com")...); err != nil {
		return nil, fmt.Errorf("create Natural Language client: %w", err)
	}
	if c.document, err = documentai.NewService(ctx, opts("documentai.googleapis.com")...); err != nil {
		return nil, fmt.Errorf("create Document AI client: %w", err)
	}
	if c.dialog, err = dialogflow.NewService(ctx, opts("dialogflow.googleapis.com")...); err != nil {
		return nil, fmt.Errorf("create Dialogflow CX client: %w", err)
	}
	if c.vertex, err = aiplatform.NewService(ctx, opts("aiplatform.googleapis.com")...); err != nil {
		return nil, fmt.Errorf("create AI Platform client: %w", err)
	}
	return &c, nil
}

func proveDefaultGate(ctx context.Context, c *clients, cfg config) error {
	parent := locationParent(cfg)
	checks := []struct {
		name string
		call func() error
	}{
		{"Speech", func() error { _, err := c.speech.Speech.Recognize(speechRequest("gate")).Context(ctx).Do(); return err }},
		{"Text-to-Speech", func() error { _, err := c.tts.Text.Synthesize(ttsRequest("gate")).Context(ctx).Do(); return err }},
		{"Translation", func() error {
			_, err := c.translate.Projects.Locations.TranslateText(parent, translationRequest("gate", "en", "en")).Context(ctx).Do()
			return err
		}},
		{"Vision", func() error {
			_, err := annotateVision(ctx, c.vision, visionRequest("gate"), cfg.project)
			return err
		}},
		{"Natural Language", func() error {
			_, err := c.language.Documents.AnalyzeSentiment(languageRequest("gate")).Context(ctx).Do()
			return err
		}},
		{"Document AI", func() error {
			_, err := c.document.Projects.Locations.Processors.List(parent).Context(ctx).Do()
			return err
		}},
		{"Dialogflow CX", func() error { _, err := c.dialog.Projects.Locations.Agents.List(parent).Context(ctx).Do(); return err }},
		{"AI Platform", func() error { _, err := c.vertex.Projects.Locations.Indexes.List(parent).Context(ctx).Do(); return err }},
	}
	for _, check := range checks {
		if err := expectGoogleStatus(check.call(), 501, "UNIMPLEMENTED"); err != nil {
			return fmt.Errorf("%s default experimental gate: %w", check.name, err)
		}
	}
	fmt.Println("default-disabled Phase 23 gate verified with eight generated clients")
	return nil
}

func createAndRecord(ctx context.Context, c *clients, cfg config) error {
	parent := locationParent(cfg)
	if err := proveSemanticAndBoundedBehavior(ctx, c, cfg); err != nil {
		return err
	}

	processor, err := c.document.Projects.Locations.Processors.Create(parent,
		&documentai.GoogleCloudDocumentaiV1Processor{Type: "OCR_PROCESSOR", DisplayName: "Phase 23"}).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("create Document AI processor: %w", err)
	}
	agent, err := c.dialog.Projects.Locations.Agents.Create(parent,
		&dialogflow.GoogleCloudDialogflowCxV3Agent{
			DisplayName: "Phase 23", DefaultLanguageCode: "en", TimeZone: "UTC",
		}).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("create Dialogflow CX agent: %w", err)
	}
	indexOp, err := c.vertex.Projects.Locations.Indexes.Create(parent,
		&aiplatform.GoogleCloudAiplatformV1Index{DisplayName: "Phase 23 index"}).Context(ctx).Do()
	if err != nil || indexOp == nil || !indexOp.Done {
		return fmt.Errorf("create Vertex index operation done=%t: %w", indexOp != nil && indexOp.Done, err)
	}
	modelOp, err := c.vertex.Projects.Locations.Models.Upload(parent,
		&aiplatform.GoogleCloudAiplatformV1UploadModelRequest{
			Model: &aiplatform.GoogleCloudAiplatformV1Model{DisplayName: "Phase 23 model"},
		}).Context(ctx).Do()
	if err != nil || modelOp == nil || !modelOp.Done {
		return fmt.Errorf("upload Vertex model operation done=%t: %w", modelOp != nil && modelOp.Done, err)
	}
	indexName, err := operationTarget(indexOp.Metadata)
	if err != nil {
		return fmt.Errorf("Vertex index operation metadata: %w", err)
	}
	modelName, err := operationTarget(modelOp.Metadata)
	if err != nil {
		return fmt.Errorf("Vertex model operation metadata: %w", err)
	}
	if err := proveControlPlaneBoundaries(ctx, c, cfg, processor.Name, agent.Name, indexName); err != nil {
		return err
	}
	for label, call := range map[string]func() error{
		"Document AI processor": func() error {
			got, err := c.document.Projects.Locations.Processors.Get(processor.Name).Context(ctx).Do()
			return namedResult(processor.Name, documentProcessorName(got), err)
		},
		"Dialogflow CX agent": func() error {
			got, err := c.dialog.Projects.Locations.Agents.Get(agent.Name).Context(ctx).Do()
			return namedResult(agent.Name, dialogAgentName(got), err)
		},
		"Vertex index": func() error {
			got, err := c.vertex.Projects.Locations.Indexes.Get(indexName).Context(ctx).Do()
			return namedResult(indexName, vertexIndexName(got), err)
		},
		"Vertex model": func() error {
			got, err := c.vertex.Projects.Locations.Models.Get(modelName).Context(ctx).Do()
			return namedResult(modelName, vertexModelName(got), err)
		},
	} {
		if err := call(); err != nil {
			return fmt.Errorf("get created %s: %w", label, err)
		}
	}
	if err := proveProjectIsolation(ctx, c, cfg); err != nil {
		return err
	}
	record := evidence{
		Version: evidenceVersion, GoogleAPIVersion: googleAPIVersion,
		Project: cfg.project, Location: cfg.location,
		DocumentProcessor: processor.Name, DialogflowAgent: agent.Name,
		VertexIndex: indexName, VertexModel: modelName,
		VertexIndexOperation: indexOp.Name, VertexModelOperation: modelOp.Name,
		IdentityTranslation: true, SemanticBoundaries: "EXPLICIT_UNIMPLEMENTED",
		PayloadBounds: "VERIFIED", UnsupportedOptions: "EXPLICIT_UNIMPLEMENTED",
		ProjectIsolation: true, LocalExtensions: "NONE",
	}
	if err := writeEvidence(cfg.evidencePath, record, cfg.sensitiveSentinel); err != nil {
		return err
	}
	fmt.Println("Phase 23 generated clients proved bounded semantic and restartable control-plane slices; local extensions: NONE")
	return nil
}

func proveSemanticAndBoundedBehavior(ctx context.Context, c *clients, cfg config) error {
	parent := locationParent(cfg)
	sentinel := cfg.sensitiveSentinel
	unimplemented := []struct {
		name string
		call func() error
	}{
		{"Speech recognition", func() error {
			_, err := c.speech.Speech.Recognize(speechRequest(sentinel)).Context(ctx).Do()
			return err
		}},
		{"Text-to-Speech synthesis", func() error { _, err := c.tts.Text.Synthesize(ttsRequest(sentinel)).Context(ctx).Do(); return err }},
		{"cross-language translation", func() error {
			_, err := c.translate.Projects.Locations.TranslateText(parent, translationRequest(sentinel, "en", "fr")).Context(ctx).Do()
			return err
		}},
		{"Vision annotation", func() error {
			_, err := annotateVision(ctx, c.vision, visionRequest(sentinel), cfg.project)
			return err
		}},
		{"Natural Language sentiment", func() error {
			_, err := c.language.Documents.AnalyzeSentiment(languageRequest(sentinel)).Context(ctx).Do()
			return err
		}},
	}
	for _, check := range unimplemented {
		if err := expectGoogleStatus(check.call(), 501, "UNIMPLEMENTED"); err != nil {
			return fmt.Errorf("%s boundary: %w", check.name, err)
		}
	}
	identity, err := c.translate.Projects.Locations.TranslateText(parent,
		translationRequest(sentinel, "en", "en")).Context(ctx).Do()
	if err != nil || len(identity.Translations) != 1 || identity.Translations[0].TranslatedText != sentinel {
		return fmt.Errorf("identity translation was not deterministic: %w", err)
	}

	unsupported := []struct {
		name string
		call func() error
	}{
		{"Speech model", func() error {
			request := speechRequest(sentinel)
			request.Config.Model = "latest_long"
			_, err := c.speech.Speech.Recognize(request).Context(ctx).Do()
			return err
		}},
		{"Text-to-Speech named voice", func() error {
			request := ttsRequest(sentinel)
			request.Voice.Name = "en-US-Neural2-A"
			_, err := c.tts.Text.Synthesize(request).Context(ctx).Do()
			return err
		}},
		{"Translation model", func() error {
			request := translationRequest(sentinel, "en", "en")
			request.Model = parent + "/models/general/nmt"
			_, err := c.translate.Projects.Locations.TranslateText(parent, request).Context(ctx).Do()
			return err
		}},
		{"Vision image context", func() error {
			request := visionRequest(sentinel)
			request.Requests[0].ImageContext = &vision.ImageContext{LanguageHints: []string{"en"}}
			_, err := annotateVision(ctx, c.vision, request, cfg.project)
			return err
		}},
		{"Natural Language encoding", func() error {
			request := languageRequest(sentinel)
			request.EncodingType = "UTF8"
			_, err := c.language.Documents.AnalyzeSentiment(request).Context(ctx).Do()
			return err
		}},
	}
	for _, check := range unsupported {
		if err := expectGoogleStatus(check.call(), 501, "UNIMPLEMENTED"); err != nil {
			return fmt.Errorf("%s option boundary: %w", check.name, err)
		}
	}
	oversizedTTS := ttsRequest(strings.Repeat("x", 5001))
	_, err = c.tts.Text.Synthesize(oversizedTTS).Context(ctx).Do()
	if statusErr := expectGoogleStatus(err, 400, "INVALID_ARGUMENT"); statusErr != nil {
		return fmt.Errorf("Text-to-Speech payload bound: %w", statusErr)
	}
	oversizedSpeech := speechRequest(strings.Repeat("x", (512<<10)+1))
	_, err = c.speech.Speech.Recognize(oversizedSpeech).Context(ctx).Do()
	if statusErr := expectGoogleStatus(err, 400, "INVALID_ARGUMENT"); statusErr != nil {
		return fmt.Errorf("Speech payload bound: %w", statusErr)
	}
	tooMany := translationRequest("", "en", "en")
	tooMany.Contents = make([]string, 129)
	_, err = c.translate.Projects.Locations.TranslateText(parent, tooMany).Context(ctx).Do()
	if statusErr := expectGoogleStatus(err, 400, "INVALID_ARGUMENT"); statusErr != nil {
		return fmt.Errorf("Translation item bound: %w", statusErr)
	}
	visionBatch := visionRequest("x")
	for len(visionBatch.Requests) < 17 {
		visionBatch.Requests = append(visionBatch.Requests, visionRequest("x").Requests[0])
	}
	_, err = annotateVision(ctx, c.vision, visionBatch, cfg.project)
	if statusErr := expectGoogleStatus(err, 400, "INVALID_ARGUMENT"); statusErr != nil {
		return fmt.Errorf("Vision batch bound: %w", statusErr)
	}
	_, err = c.language.Documents.AnalyzeSentiment(
		languageRequest(strings.Repeat("x", 100_001))).Context(ctx).Do()
	if statusErr := expectGoogleStatus(err, 400, "INVALID_ARGUMENT"); statusErr != nil {
		return fmt.Errorf("Natural Language payload bound: %w", statusErr)
	}
	return nil
}

func proveControlPlaneBoundaries(
	ctx context.Context,
	c *clients,
	cfg config,
	processorName, agentName, indexName string,
) error {
	documentRequest := &documentai.GoogleCloudDocumentaiV1ProcessRequest{
		RawDocument: &documentai.GoogleCloudDocumentaiV1RawDocument{
			Content:  base64.StdEncoding.EncodeToString([]byte(cfg.sensitiveSentinel)),
			MimeType: "application/pdf",
		},
	}
	_, err := c.document.Projects.Locations.Processors.Process(processorName, documentRequest).Context(ctx).Do()
	if statusErr := expectGoogleStatus(err, 501, "UNIMPLEMENTED"); statusErr != nil {
		return fmt.Errorf("Document AI semantic boundary: %w", statusErr)
	}
	inlineRequest := &documentai.GoogleCloudDocumentaiV1ProcessRequest{
		InlineDocument: &documentai.GoogleCloudDocumentaiV1Document{Text: cfg.sensitiveSentinel},
	}
	_, err = c.document.Projects.Locations.Processors.Process(processorName, inlineRequest).Context(ctx).Do()
	if statusErr := expectGoogleStatus(err, 501, "UNIMPLEMENTED"); statusErr != nil {
		return fmt.Errorf("Document AI inline option boundary: %w", statusErr)
	}
	oversizedDocument := &documentai.GoogleCloudDocumentaiV1ProcessRequest{
		RawDocument: &documentai.GoogleCloudDocumentaiV1RawDocument{
			Content:  base64.StdEncoding.EncodeToString(make([]byte, (5<<20)+1)),
			MimeType: "application/pdf",
		},
	}
	_, err = c.document.Projects.Locations.Processors.Process(processorName, oversizedDocument).Context(ctx).Do()
	if statusErr := expectGoogleStatus(err, 413, "INVALID_ARGUMENT"); statusErr != nil {
		return fmt.Errorf("Document AI payload bound: %w", statusErr)
	}

	session := agentName + "/sessions/phase23"
	detectRequest := &dialogflow.GoogleCloudDialogflowCxV3DetectIntentRequest{
		QueryInput: &dialogflow.GoogleCloudDialogflowCxV3QueryInput{
			LanguageCode: "en",
			Text:         &dialogflow.GoogleCloudDialogflowCxV3TextInput{Text: cfg.sensitiveSentinel},
		},
	}
	_, err = c.dialog.Projects.Locations.Agents.Sessions.DetectIntent(session, detectRequest).Context(ctx).Do()
	if statusErr := expectGoogleStatus(err, 501, "UNIMPLEMENTED"); statusErr != nil {
		return fmt.Errorf("Dialogflow CX semantic boundary: %w", statusErr)
	}
	oversizedDialog := &dialogflow.GoogleCloudDialogflowCxV3DetectIntentRequest{
		QueryInput: &dialogflow.GoogleCloudDialogflowCxV3QueryInput{
			LanguageCode: "en",
			Text:         &dialogflow.GoogleCloudDialogflowCxV3TextInput{Text: strings.Repeat("x", 4097)},
		},
	}
	_, err = c.dialog.Projects.Locations.Agents.Sessions.DetectIntent(session, oversizedDialog).Context(ctx).Do()
	if statusErr := expectGoogleStatus(err, 400, "INVALID_ARGUMENT"); statusErr != nil {
		return fmt.Errorf("Dialogflow CX payload bound: %w", statusErr)
	}
	audioDialog := &dialogflow.GoogleCloudDialogflowCxV3DetectIntentRequest{
		QueryInput: &dialogflow.GoogleCloudDialogflowCxV3QueryInput{
			LanguageCode: "en",
			Audio:        &dialogflow.GoogleCloudDialogflowCxV3AudioInput{},
		},
	}
	_, err = c.dialog.Projects.Locations.Agents.Sessions.DetectIntent(session, audioDialog).Context(ctx).Do()
	if statusErr := expectGoogleStatus(err, 501, "UNIMPLEMENTED"); statusErr != nil {
		return fmt.Errorf("Dialogflow CX audio option boundary: %w", statusErr)
	}

	parent := locationParent(cfg)
	_, err = c.vertex.Projects.Locations.Indexes.Create(parent,
		&aiplatform.GoogleCloudAiplatformV1Index{
			DisplayName: "unsupported metadata",
			Metadata:    map[string]any{"dimensions": 2},
		}).Context(ctx).Do()
	if statusErr := expectGoogleStatus(err, 501, "UNIMPLEMENTED"); statusErr != nil {
		return fmt.Errorf("Vertex metadata option boundary: %w", statusErr)
	}
	_, err = c.vertex.Projects.Locations.Indexes.Patch(indexName,
		&aiplatform.GoogleCloudAiplatformV1Index{DisplayName: "updated"}).Context(ctx).Do()
	if statusErr := expectGoogleStatus(err, 501, "UNIMPLEMENTED"); statusErr != nil {
		return fmt.Errorf("Vertex index update boundary: %w", statusErr)
	}
	_, err = c.vertex.Projects.Locations.Indexes.Create(parent,
		&aiplatform.GoogleCloudAiplatformV1Index{
			DisplayName: "oversized", Description: strings.Repeat("x", (1<<20)+1),
		}).Context(ctx).Do()
	if statusErr := expectGoogleStatus(err, 413, "INVALID_ARGUMENT"); statusErr != nil {
		return fmt.Errorf("Vertex request-body bound: %w", statusErr)
	}
	return nil
}

func proveProjectIsolation(ctx context.Context, c *clients, cfg config) error {
	other := "projects/" + cfg.project + "-other/locations/" + cfg.location
	processors, err := c.document.Projects.Locations.Processors.List(other).Context(ctx).Do()
	if err != nil || len(processors.Processors) != 0 {
		return fmt.Errorf("Document AI project isolation count=%d: %w", len(processors.Processors), err)
	}
	agents, err := c.dialog.Projects.Locations.Agents.List(other).Context(ctx).Do()
	if err != nil || len(agents.Agents) != 0 {
		return fmt.Errorf("Dialogflow project isolation count=%d: %w", len(agents.Agents), err)
	}
	indexes, err := c.vertex.Projects.Locations.Indexes.List(other).Context(ctx).Do()
	if err != nil || len(indexes.Indexes) != 0 {
		return fmt.Errorf("Vertex index project isolation count=%d: %w", len(indexes.Indexes), err)
	}
	models, err := c.vertex.Projects.Locations.Models.List(other).Context(ctx).Do()
	if err != nil || len(models.Models) != 0 {
		return fmt.Errorf("Vertex model project isolation count=%d: %w", len(models.Models), err)
	}
	return nil
}

func verifyRestart(ctx context.Context, c *clients, cfg config) error {
	record, err := readEvidence(cfg.evidencePath, cfg, cfg.sensitiveSentinel)
	if err != nil {
		return err
	}
	checks := map[string]func() error{
		"Document AI processor": func() error {
			got, err := c.document.Projects.Locations.Processors.Get(record.DocumentProcessor).Context(ctx).Do()
			return namedResult(record.DocumentProcessor, documentProcessorName(got), err)
		},
		"Dialogflow CX agent": func() error {
			got, err := c.dialog.Projects.Locations.Agents.Get(record.DialogflowAgent).Context(ctx).Do()
			return namedResult(record.DialogflowAgent, dialogAgentName(got), err)
		},
		"Vertex index": func() error {
			got, err := c.vertex.Projects.Locations.Indexes.Get(record.VertexIndex).Context(ctx).Do()
			return namedResult(record.VertexIndex, vertexIndexName(got), err)
		},
		"Vertex model": func() error {
			got, err := c.vertex.Projects.Locations.Models.Get(record.VertexModel).Context(ctx).Do()
			return namedResult(record.VertexModel, vertexModelName(got), err)
		},
		"Vertex index operation": func() error {
			op, err := c.vertex.Projects.Locations.Operations.Get(record.VertexIndexOperation).Context(ctx).Do()
			if err == nil && (op == nil || !op.Done) {
				return errors.New("operation is not done")
			}
			return err
		},
		"Vertex model operation": func() error {
			op, err := c.vertex.Projects.Locations.Operations.Get(record.VertexModelOperation).Context(ctx).Do()
			if err == nil && (op == nil || !op.Done) {
				return errors.New("operation is not done")
			}
			return err
		},
	}
	for label, check := range checks {
		if err := check(); err != nil {
			return fmt.Errorf("restart %s: %w", label, err)
		}
	}
	if err := proveProjectIsolation(ctx, c, cfg); err != nil {
		return err
	}
	fmt.Println("Phase 23 generated-client restart and project-isolation verification passed")
	return nil
}

func deleteAndVerify(ctx context.Context, c *clients, cfg config) error {
	record, err := readEvidence(cfg.evidencePath, cfg, cfg.sensitiveSentinel)
	if err != nil {
		return err
	}
	documentOp, err := c.document.Projects.Locations.Processors.Delete(record.DocumentProcessor).Context(ctx).Do()
	if err != nil || documentOp == nil || !documentOp.Done {
		return fmt.Errorf("delete Document AI processor done=%t: %w", documentOp != nil && documentOp.Done, err)
	}
	record.DocumentDeleteOperation = documentOp.Name
	if _, err := c.dialog.Projects.Locations.Agents.Delete(record.DialogflowAgent).Context(ctx).Do(); err != nil {
		return fmt.Errorf("delete Dialogflow CX agent: %w", err)
	}
	indexOp, err := c.vertex.Projects.Locations.Indexes.Delete(record.VertexIndex).Context(ctx).Do()
	if err != nil || indexOp == nil || !indexOp.Done {
		return fmt.Errorf("delete Vertex index done=%t: %w", indexOp != nil && indexOp.Done, err)
	}
	modelOp, err := c.vertex.Projects.Locations.Models.Delete(record.VertexModel).Context(ctx).Do()
	if err != nil || modelOp == nil || !modelOp.Done {
		return fmt.Errorf("delete Vertex model done=%t: %w", modelOp != nil && modelOp.Done, err)
	}
	checks := map[string]func() error{
		"Document AI processor": func() error {
			_, err := c.document.Projects.Locations.Processors.Get(record.DocumentProcessor).Context(ctx).Do()
			return err
		},
		"Dialogflow CX agent": func() error {
			_, err := c.dialog.Projects.Locations.Agents.Get(record.DialogflowAgent).Context(ctx).Do()
			return err
		},
		"Vertex index": func() error {
			_, err := c.vertex.Projects.Locations.Indexes.Get(record.VertexIndex).Context(ctx).Do()
			return err
		},
		"Vertex model": func() error {
			_, err := c.vertex.Projects.Locations.Models.Get(record.VertexModel).Context(ctx).Do()
			return err
		},
	}
	for label, check := range checks {
		if err := expectGoogleStatus(check(), 404, "NOT_FOUND"); err != nil {
			return fmt.Errorf("deleted %s: %w", label, err)
		}
	}
	if err := writeEvidence(cfg.evidencePath, record, cfg.sensitiveSentinel); err != nil {
		return err
	}
	fmt.Println("Phase 23 generated-client control-plane delete/404 cleanup passed; local extensions: NONE")
	return nil
}

func speechRequest(value string) *speech.RecognizeRequest {
	return &speech.RecognizeRequest{
		Config: &speech.RecognitionConfig{LanguageCode: "en-US"},
		Audio:  &speech.RecognitionAudio{Content: base64.StdEncoding.EncodeToString([]byte(value))},
	}
}

func ttsRequest(value string) *texttospeech.SynthesizeSpeechRequest {
	return &texttospeech.SynthesizeSpeechRequest{
		Input:       &texttospeech.SynthesisInput{Text: value},
		Voice:       &texttospeech.VoiceSelectionParams{LanguageCode: "en-US"},
		AudioConfig: &texttospeech.AudioConfig{AudioEncoding: "LINEAR16"},
	}
}

func translationRequest(value, source, target string) *translate.TranslateTextRequest {
	return &translate.TranslateTextRequest{
		Contents: []string{value}, SourceLanguageCode: source,
		TargetLanguageCode: target, MimeType: "text/plain",
	}
}

func visionRequest(value string) *vision.BatchAnnotateImagesRequest {
	return &vision.BatchAnnotateImagesRequest{Requests: []*vision.AnnotateImageRequest{{
		Image:    &vision.Image{Content: base64.StdEncoding.EncodeToString([]byte(value))},
		Features: []*vision.Feature{{Type: "TEXT_DETECTION"}},
	}}}
}

func annotateVision(
	ctx context.Context,
	service *vision.Service,
	request *vision.BatchAnnotateImagesRequest,
	project string,
) (*vision.BatchAnnotateImagesResponse, error) {
	call := service.Images.Annotate(request)
	// images:annotate has no project path segment, so the public gateway uses
	// the standard quota-project header for project isolation.
	call.Header().Set("X-Goog-User-Project", project)
	return call.Context(ctx).Do()
}

func languageRequest(value string) *language.AnalyzeSentimentRequest {
	return &language.AnalyzeSentimentRequest{
		Document: &language.Document{Type: "PLAIN_TEXT", Content: value, Language: "en"},
	}
}

func operationTarget(metadata any) (string, error) {
	raw, err := json.Marshal(metadata)
	if err != nil {
		return "", err
	}
	var value struct {
		Target string `json:"target"`
	}
	if err := json.Unmarshal(raw, &value); err != nil || value.Target == "" {
		return "", errors.New("operation metadata omitted target")
	}
	return value.Target, nil
}

func expectGoogleStatus(err error, code int, status string) error {
	if err == nil {
		return fmt.Errorf("expected HTTP %d %s, got success", code, status)
	}
	var apiErr *googleapi.Error
	if !errors.As(err, &apiErr) || apiErr.Code != code {
		return fmt.Errorf("expected googleapi.Error HTTP %d, got %T: %w", code, err, err)
	}
	var envelope struct {
		Error struct {
			Status string `json:"status"`
		} `json:"error"`
	}
	if json.Unmarshal([]byte(apiErr.Body), &envelope) != nil || envelope.Error.Status != status {
		return fmt.Errorf("status=%q want=%q body=%s", envelope.Error.Status, status, apiErr.Body)
	}
	return nil
}

func writeEvidence(path string, record evidence, sensitive string) error {
	if err := validateEvidence(record); err != nil {
		return err
	}
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	if bytesContain(data, sensitive) {
		return errors.New("refusing to persist sensitive probe content in evidence")
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".phase23-evidence-*.tmp")
	if err != nil {
		return err
	}
	name := temp.Name()
	defer os.Remove(name)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return err
	}
	if _, err := temp.Write(append(data, '\n')); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}

func readEvidence(path string, cfg config, sensitive string) (evidence, error) {
	file, err := os.Open(path)
	if err != nil {
		return evidence{}, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxEvidenceBytes+1))
	if err != nil {
		return evidence{}, err
	}
	if len(data) > maxEvidenceBytes {
		return evidence{}, errors.New("Phase 23 evidence exceeds size limit")
	}
	if bytesContain(data, sensitive) {
		return evidence{}, errors.New("Phase 23 evidence contains sensitive probe content")
	}
	var record evidence
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return evidence{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return evidence{}, errors.New("Phase 23 evidence contains trailing JSON")
	}
	if err := validateEvidence(record); err != nil {
		return evidence{}, err
	}
	if record.Project != cfg.project || record.Location != cfg.location {
		return evidence{}, errors.New("evidence identifiers do not match smoke configuration")
	}
	return record, nil
}

func validateEvidence(record evidence) error {
	if record.Version != evidenceVersion || record.GoogleAPIVersion != googleAPIVersion {
		return errors.New("invalid Phase 23 evidence version")
	}
	parent := "projects/" + record.Project + "/locations/" + record.Location
	if record.DocumentProcessor == "" || !strings.HasPrefix(record.DocumentProcessor, parent+"/processors/") ||
		record.DialogflowAgent == "" || !strings.HasPrefix(record.DialogflowAgent, parent+"/agents/") ||
		record.VertexIndex == "" || !strings.HasPrefix(record.VertexIndex, parent+"/indexes/") ||
		record.VertexModel == "" || !strings.HasPrefix(record.VertexModel, parent+"/models/") ||
		record.VertexIndexOperation == "" || !strings.HasPrefix(record.VertexIndexOperation, parent+"/operations/") ||
		record.VertexModelOperation == "" || !strings.HasPrefix(record.VertexModelOperation, parent+"/operations/") {
		return errors.New("Phase 23 evidence resource hierarchy is incomplete")
	}
	if !record.IdentityTranslation || !record.ProjectIsolation ||
		record.SemanticBoundaries != "EXPLICIT_UNIMPLEMENTED" ||
		record.PayloadBounds != "VERIFIED" ||
		record.UnsupportedOptions != "EXPLICIT_UNIMPLEMENTED" ||
		record.LocalExtensions != "NONE" {
		return errors.New("Phase 23 evidence classifications are incomplete")
	}
	if record.DocumentDeleteOperation != "" &&
		!strings.HasPrefix(record.DocumentDeleteOperation, parent+"/operations/") {
		return errors.New("Phase 23 delete operation hierarchy is invalid")
	}
	return nil
}

func bytesContain(data []byte, value string) bool {
	return value != "" && strings.Contains(string(data), value)
}

func locationParent(cfg config) string {
	return "projects/" + cfg.project + "/locations/" + cfg.location
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func namedResult(want, got string, err error) error {
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("name=%q want=%q", got, want)
	}
	return nil
}

func documentProcessorName(value *documentai.GoogleCloudDocumentaiV1Processor) string {
	if value == nil {
		return ""
	}
	return value.Name
}

func dialogAgentName(value *dialogflow.GoogleCloudDialogflowCxV3Agent) string {
	if value == nil {
		return ""
	}
	return value.Name
}

func vertexIndexName(value *aiplatform.GoogleCloudAiplatformV1Index) string {
	if value == nil {
		return ""
	}
	return value.Name
}

func vertexModelName(value *aiplatform.GoogleCloudAiplatformV1Model) string {
	if value == nil {
		return ""
	}
	return value.Name
}
