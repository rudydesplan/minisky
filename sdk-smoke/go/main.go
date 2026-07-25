package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"google.golang.org/api/bigquery/v2"
	"google.golang.org/api/iam/v1"
	"google.golang.org/api/iamcredentials/v1"
	"google.golang.org/api/option"
	"google.golang.org/api/storage/v1"
)

const defaultProjectID = "local-dev-project"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Go SDK smoke failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	gateway := strings.TrimRight(strings.TrimSpace(os.Getenv("MINISKY_ENDPOINT")), "/")
	if gateway == "" {
		return fmt.Errorf("MINISKY_ENDPOINT is required")
	}
	projectID := strings.TrimSpace(os.Getenv("MINISKY_PROJECT_ID"))
	if projectID == "" {
		projectID = defaultProjectID
	}
	secondaryProjectID := strings.TrimSpace(os.Getenv("MINISKY_SECONDARY_PROJECT_ID"))
	if secondaryProjectID == "" {
		secondaryProjectID = "local-secondary-project"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clientOptions := []option.ClientOption{
		option.WithoutAuthentication(),
	}
	bq, err := bigquery.NewService(ctx, append(clientOptions,
		option.WithEndpoint(gateway+"/_minisky/bigquery/bigquery/v2/"),
	)...)
	if err != nil {
		return fmt.Errorf("create BigQuery client: %w", err)
	}
	iamService, err := iam.NewService(ctx, append(clientOptions,
		option.WithEndpoint(gateway+"/_minisky/iam/v1/"),
	)...)
	if err != nil {
		return fmt.Errorf("create IAM client: %w", err)
	}
	credentialsService, err := iamcredentials.NewService(ctx, append(clientOptions,
		option.WithEndpoint(gateway+"/_minisky/iamcredentials/"),
	)...)
	if err != nil {
		return fmt.Errorf("create IAM Credentials client: %w", err)
	}
	storageService, err := storage.NewService(ctx, append(clientOptions,
		option.WithEndpoint(gateway+"/_minisky/storage/storage/v1/"),
	)...)
	if err != nil {
		return fmt.Errorf("create Storage client: %w", err)
	}

	suffix := time.Now().UnixNano() % 1_000_000_000
	datasetID := fmt.Sprintf("minisky_go_%09d", suffix)
	tableID := "events"
	accountID := fmt.Sprintf("minisky-go-%09d", suffix)
	accountEmail := fmt.Sprintf("%s@%s.iam.gserviceaccount.com", accountID, projectID)
	bucketName := fmt.Sprintf("minisky-go-%09d", suffix)

	datasetCreated := false
	secondaryDatasetCreated := false
	tableCreated := false
	accountCreated := false
	bucketCreated := false
	defer func() {
		if tableCreated {
			_ = bq.Tables.Delete(projectID, datasetID, tableID).Context(ctx).Do()
		}
		if datasetCreated {
			_ = bq.Datasets.Delete(projectID, datasetID).DeleteContents(true).Context(ctx).Do()
		}
		if secondaryDatasetCreated {
			_ = bq.Datasets.Delete(secondaryProjectID, datasetID).DeleteContents(true).Context(ctx).Do()
		}
		if accountCreated {
			_, _ = iamService.Projects.ServiceAccounts.Delete(
				fmt.Sprintf("projects/%s/serviceAccounts/%s", projectID, accountEmail),
			).Context(ctx).Do()
		}
		if bucketCreated {
			_ = storageService.Buckets.Delete(bucketName).Context(ctx).Do()
		}
	}()

	dataset := &bigquery.Dataset{
		DatasetReference: &bigquery.DatasetReference{
			ProjectId: projectID,
			DatasetId: datasetID,
		},
		Location: "US",
	}
	if _, err := bq.Datasets.Insert(projectID, dataset).Context(ctx).Do(); err != nil {
		return fmt.Errorf("create dataset: %w", err)
	}
	datasetCreated = true
	secondaryDataset := &bigquery.Dataset{
		DatasetReference: &bigquery.DatasetReference{ProjectId: secondaryProjectID, DatasetId: datasetID},
		Location:         "US",
	}
	if _, err := bq.Datasets.Insert(secondaryProjectID, secondaryDataset).Context(ctx).Do(); err != nil {
		return fmt.Errorf("create same-named secondary dataset: %w", err)
	}
	secondaryDatasetCreated = true

	table := &bigquery.Table{
		TableReference: &bigquery.TableReference{
			ProjectId: projectID,
			DatasetId: datasetID,
			TableId:   tableID,
		},
		Schema: &bigquery.TableSchema{Fields: []*bigquery.TableFieldSchema{
			{Name: "message", Type: "STRING", Mode: "NULLABLE"},
		}},
	}
	if _, err := bq.Tables.Insert(projectID, datasetID, table).Context(ctx).Do(); err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	tableCreated = true

	gotDataset, err := bq.Datasets.Get(projectID, datasetID).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("get dataset: %w", err)
	}
	if gotDataset.DatasetReference == nil || gotDataset.DatasetReference.DatasetId != datasetID {
		return fmt.Errorf("dataset round trip returned the wrong ID")
	}
	gotSecondary, err := bq.Datasets.Get(secondaryProjectID, datasetID).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("get secondary project dataset: %w", err)
	}
	if gotSecondary.DatasetReference == nil || gotSecondary.DatasetReference.ProjectId != secondaryProjectID {
		return fmt.Errorf("secondary project dataset isolation failed")
	}
	gotTable, err := bq.Tables.Get(projectID, datasetID, tableID).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("get table: %w", err)
	}
	if gotTable.TableReference == nil || gotTable.TableReference.TableId != tableID {
		return fmt.Errorf("table round trip returned the wrong ID")
	}

	account, err := iamService.Projects.ServiceAccounts.Create(
		"projects/"+projectID,
		&iam.CreateServiceAccountRequest{
			AccountId: accountID,
			ServiceAccount: &iam.ServiceAccount{
				DisplayName: "MiniSky Go SDK smoke",
			},
		},
	).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("create service account: %w", err)
	}
	accountCreated = true
	if account.Email != accountEmail {
		return fmt.Errorf("service account create returned %q, want %q", account.Email, accountEmail)
	}
	gotAccount, err := iamService.Projects.ServiceAccounts.Get(account.Name).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("get service account: %w", err)
	}
	if gotAccount.Email != accountEmail {
		return fmt.Errorf("service account round trip returned the wrong email")
	}
	credential, err := credentialsService.Projects.ServiceAccounts.GenerateAccessToken(
		"projects/-/serviceAccounts/"+accountEmail,
		&iamcredentials.GenerateAccessTokenRequest{
			Scope:    []string{"https://www.googleapis.com/auth/cloud-platform"},
			Lifetime: "300s",
		},
	).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("generate impersonated access token: %w", err)
	}
	if credential.AccessToken == "" || credential.ExpireTime == "" {
		return fmt.Errorf("IAM Credentials returned an incomplete token response")
	}

	bucket, err := storageService.Buckets.Insert(projectID, &storage.Bucket{
		Name:     bucketName,
		Location: "US",
	}).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("create storage bucket: %w", err)
	}
	bucketCreated = true
	if bucket.Name != bucketName {
		return fmt.Errorf("storage bucket create returned %q, want %q", bucket.Name, bucketName)
	}
	gotBucket, err := storageService.Buckets.Get(bucketName).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("get storage bucket: %w", err)
	}
	if gotBucket.Name != bucketName {
		return fmt.Errorf("storage bucket round trip returned the wrong name")
	}

	fmt.Printf("Go SDK smoke passed: dataset=%s table=%s service_account=%s bucket=%s\n",
		datasetID, tableID, accountEmail, bucketName)
	return nil
}
