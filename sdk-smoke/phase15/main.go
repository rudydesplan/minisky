package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/firestore"
	"cloud.google.com/go/spanner"
	databaseadmin "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instanceadmin "cloud.google.com/go/spanner/admin/instance/apiv1"
	instancepb "cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	project := env("MINISKY_PROJECT_ID", "local-dev-project")
	must(firestoreSmoke(ctx, project))
	must(datastoreSmoke(ctx, project))
	must(spannerSmoke(ctx, project))
	fmt.Println("phase 15 emulator SDK smoke passed")
}

func firestoreSmoke(ctx context.Context, project string) error {
	client, err := firestore.NewClient(ctx, project)
	if err != nil {
		return fmt.Errorf("create Firestore client: %w", err)
	}
	defer client.Close()
	collection := client.Collection("minisky_phase15")
	if _, err := collection.Doc("one").Set(ctx, map[string]any{"group": "a", "value": 1}); err != nil {
		return fmt.Errorf("Firestore set: %w", err)
	}
	snapshot, err := collection.Doc("one").Get(ctx)
	if err != nil {
		return fmt.Errorf("Firestore get: %w", err)
	}
	if value, err := snapshot.DataAt("value"); err != nil || value.(int64) != 1 {
		return fmt.Errorf("Firestore document value = %v, err = %v", value, err)
	}
	iterator := collection.Where("group", "==", "a").Documents(ctx)
	defer iterator.Stop()
	if _, err := iterator.Next(); err != nil {
		return fmt.Errorf("Firestore query: %w", err)
	}
	if _, err := collection.Doc("one").Delete(ctx); err != nil {
		return fmt.Errorf("Firestore delete: %w", err)
	}
	return nil
}

func datastoreSmoke(ctx context.Context, project string) error {
	client, err := datastore.NewClient(ctx, project)
	if err != nil {
		return fmt.Errorf("create Datastore client: %w", err)
	}
	defer client.Close()
	parent := datastore.NameKey("MiniSkyGroup", "a", nil)
	key := datastore.NameKey("MiniSkyEntity", "one", parent)
	entity := &datastoreEntity{Group: "a", Value: 1}
	if _, err := client.Put(ctx, key, entity); err != nil {
		return fmt.Errorf("Datastore put: %w", err)
	}
	var loaded datastoreEntity
	if err := client.Get(ctx, key, &loaded); err != nil {
		return fmt.Errorf("Datastore get: %w", err)
	}
	if loaded.Value != 1 {
		return fmt.Errorf("Datastore value = %d", loaded.Value)
	}
	var ancestors []datastoreEntity
	keys, err := client.GetAll(ctx, datastore.NewQuery("MiniSkyEntity").Ancestor(parent), &ancestors)
	if err != nil {
		return fmt.Errorf("Datastore ancestor query: %w", err)
	}
	if len(keys) != 1 {
		return fmt.Errorf("Datastore ancestor query returned %d entities", len(keys))
	}
	if err := client.Delete(ctx, key); err != nil {
		return fmt.Errorf("Datastore delete: %w", err)
	}
	return nil
}

type datastoreEntity struct {
	Group string
	Value int
}

func spannerSmoke(ctx context.Context, project string) error {
	instanceID := env("MINISKY_SPANNER_INSTANCE", "minisky-phase15")
	databaseID := env("MINISKY_SPANNER_DATABASE", "phase15")
	instanceName := fmt.Sprintf("projects/%s/instances/%s", project, instanceID)
	databaseName := fmt.Sprintf("%s/databases/%s", instanceName, databaseID)

	instanceClient, err := instanceadmin.NewInstanceAdminClient(ctx)
	if err != nil {
		return fmt.Errorf("create Spanner instance admin client: %w", err)
	}
	defer instanceClient.Close()
	instanceOperation, err := instanceClient.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     "projects/" + project,
		InstanceId: instanceID,
		Instance: &instancepb.Instance{
			Name:        instanceName,
			Config:      "projects/" + project + "/instanceConfigs/emulator-config",
			DisplayName: "MiniSky phase 15",
			NodeCount:   1,
		},
	})
	if err != nil {
		return fmt.Errorf("Spanner create instance: %w", err)
	}
	if _, err := instanceOperation.Wait(ctx); err != nil {
		return fmt.Errorf("Spanner wait instance: %w", err)
	}
	defer instanceClient.DeleteInstance(context.Background(), &instancepb.DeleteInstanceRequest{Name: instanceName})

	databaseClient, err := databaseadmin.NewDatabaseAdminClient(ctx)
	if err != nil {
		return fmt.Errorf("create Spanner database admin client: %w", err)
	}
	defer databaseClient.Close()
	databaseOperation, err := databaseClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          instanceName,
		CreateStatement: "CREATE DATABASE `" + databaseID + "`",
		ExtraStatements: []string{"CREATE TABLE Entries (Id STRING(64) NOT NULL, Value STRING(MAX)) PRIMARY KEY (Id)"},
	})
	if err != nil {
		return fmt.Errorf("Spanner create database: %w", err)
	}
	if _, err := databaseOperation.Wait(ctx); err != nil {
		return fmt.Errorf("Spanner wait database: %w", err)
	}
	defer databaseClient.DropDatabase(context.Background(), &databasepb.DropDatabaseRequest{Database: databaseName})

	dataClient, err := spanner.NewClient(ctx, databaseName)
	if err != nil {
		return fmt.Errorf("create Spanner data client: %w", err)
	}
	defer dataClient.Close()
	if _, err := dataClient.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("Entries", []string{"Id", "Value"}, []any{"one", "hello"}),
	}); err != nil {
		return fmt.Errorf("Spanner insert: %w", err)
	}
	row, err := dataClient.Single().ReadRow(ctx, "Entries", spanner.Key{"one"}, []string{"Value"})
	if err != nil {
		return fmt.Errorf("Spanner query: %w", err)
	}
	var value string
	if err := row.Columns(&value); err != nil || value != "hello" {
		return fmt.Errorf("Spanner value = %q, err = %v", value, err)
	}
	if _, err := dataClient.Apply(ctx, []*spanner.Mutation{spanner.Delete("Entries", spanner.Key{"one"})}); err != nil {
		return fmt.Errorf("Spanner delete row: %w", err)
	}
	return nil
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
