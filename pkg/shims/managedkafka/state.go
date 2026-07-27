package managedkafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"minisky/pkg/state"
)

const managedKafkaStateEntry = "managedkafka/metadata"

func init() {
	state.MustRegisterEntryValidator(managedKafkaStateEntry, state.StrictEntryValidator(validateManagedKafkaMetadata))
}

type managedKafkaStateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type managedKafkaMetadata struct {
	Clusters map[string]*Cluster `json:"clusters"`
	Topics   map[string]*Topic   `json:"topics"`
}

func validateManagedKafkaMetadata(_ state.EntryValidationContext, metadata *managedKafkaMetadata) error {
	for name := range metadata.Topics {
		index := strings.LastIndex(name, "/topics/")
		if index < 0 {
			return fmt.Errorf("topic %q has invalid parent hierarchy", name)
		}
		if _, ok := metadata.Clusters[name[:index]]; !ok {
			return fmt.Errorf("topic %q references missing cluster", name)
		}
	}
	return nil
}

// persistState deep-copies clusters and topics and writes to durable storage.
func (api *API) persistState() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	api.mu.RLock()
	clusterSnapshot := make(map[string]*Cluster, len(api.clusters))
	for k, v := range api.clusters {
		clusterSnapshot[k] = deepCopyCluster(v)
	}
	topicSnapshot := make(map[string]*Topic, len(api.topics))
	for k, v := range api.topics {
		topicSnapshot[k] = deepCopyTopic(v)
	}
	api.mu.RUnlock()

	return api.stateStore.Save(managedKafkaStateEntry, managedKafkaMetadata{
		Clusters: clusterSnapshot,
		Topics:   topicSnapshot,
	})
}

// loadState rehydrates clusters and topics from durable storage.
func (api *API) loadState() error {
	if api.stateStore == nil {
		return nil
	}
	var meta managedKafkaMetadata
	if err := api.stateStore.Load(managedKafkaStateEntry, &meta); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return nil
		}
		return err
	}
	if meta.Clusters != nil {
		api.clusters = make(map[string]*Cluster, len(meta.Clusters))
		for name, cluster := range meta.Clusters {
			restored := deepCopyCluster(cluster)
			restored.State = "FAILED"
			restored.BootstrapAddress = ""
			if api.backend != nil {
				if bootstrap, provisionErr := api.backend.Provision(context.Background(), name); provisionErr == nil {
					restored.State = "ACTIVE"
					restored.BootstrapAddress = bootstrap
				}
			}
			api.clusters[name] = restored
		}
	}
	if meta.Topics != nil {
		api.topics = meta.Topics
	}
	return nil
}

// deepCopyCluster returns a fully independent copy of a Cluster.
func deepCopyCluster(c *Cluster) *Cluster {
	raw, _ := json.Marshal(c)
	var clone Cluster
	_ = json.Unmarshal(raw, &clone)
	return &clone
}

// deepCopyTopic returns a fully independent copy of a Topic.
func deepCopyTopic(t *Topic) *Topic {
	raw, _ := json.Marshal(t)
	var clone Topic
	_ = json.Unmarshal(raw, &clone)
	return &clone
}
