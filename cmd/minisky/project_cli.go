package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"minisky/pkg/config"

	"github.com/spf13/cobra"
)

const defaultProject = "local-dev-project"

type cliProjectConfig struct {
	DefaultProject string `json:"defaultProject"`
}

type projectResource struct {
	Name        string `json:"name"`
	ProjectID   string `json:"projectId"`
	DisplayName string `json:"displayName,omitempty"`
	Parent      string `json:"parent,omitempty"`
	State       string `json:"state"`
}

var projectCmd = newProjectCommand()

func newProjectCommand() *cobra.Command {
	command := &cobra.Command{Use: "project", Short: "Manage profile-local GCP projects"}

	var displayName, parent string
	create := &cobra.Command{
		Use:   "create PROJECT_ID",
		Short: "Create a project in the active MiniSky profile",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			endpoint, err := miniskyAPIURL("cloudresourcemanager", "/v3/projects")
			if err != nil {
				return err
			}
			var operation struct {
				Response projectResource `json:"response"`
			}
			if err := postJSON(endpoint, map[string]string{
				"projectId": args[0], "displayName": displayName, "parent": parent,
			}, &operation); err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), operation.Response.ProjectID)
			return nil
		},
	}
	create.Flags().StringVar(&displayName, "display-name", "", "Project display name")
	create.Flags().StringVar(&parent, "parent", "", "Optional folders/{id} or organizations/{id} parent")

	list := &cobra.Command{
		Use:   "list",
		Short: "List projects in the active MiniSky profile",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			endpoint, err := miniskyAPIURL("cloudresourcemanager", "/v3/projects")
			if err != nil {
				return err
			}
			var response struct {
				Projects []projectResource `json:"projects"`
			}
			if err := getJSON(endpoint, &response); err != nil {
				return err
			}
			sort.Slice(response.Projects, func(i, j int) bool {
				return response.Projects[i].ProjectID < response.Projects[j].ProjectID
			})
			active := activeProjectID()
			for _, project := range response.Projects {
				marker := " "
				if project.ProjectID == active {
					marker = "*"
				}
				fmt.Fprintf(cmd.OutOrStdout(), "%s %s\t%s\n", marker, project.ProjectID, project.State)
			}
			return nil
		},
	}

	switchCommand := &cobra.Command{
		Use:   "switch PROJECT_ID",
		Short: "Persist the CLI default project for this profile",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			endpoint, err := miniskyAPIURL("cloudresourcemanager", "/v3/projects/"+args[0])
			if err != nil {
				return err
			}
			var project projectResource
			if err := getJSON(endpoint, &project); err != nil {
				return err
			}
			if err := saveCLIProjectConfig(cliProjectConfig{DefaultProject: project.ProjectID}); err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), project.ProjectID)
			return nil
		},
	}

	deleteCommand := &cobra.Command{
		Use:   "delete PROJECT_ID",
		Short: "Delete a project registry entry",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			endpoint, err := miniskyAPIURL("cloudresourcemanager", "/v3/projects/"+args[0])
			if err != nil {
				return err
			}
			if err := requestJSON("DELETE", endpoint, nil, nil); err != nil {
				return err
			}
			if activeProjectID() == args[0] {
				if err := saveCLIProjectConfig(cliProjectConfig{DefaultProject: defaultProject}); err != nil {
					return err
				}
			}
			fmt.Fprintln(cmd.OutOrStdout(), args[0])
			return nil
		},
	}

	command.AddCommand(create, list, switchCommand, deleteCommand)
	return command
}

func activeProjectID() string {
	if value := strings.TrimSpace(os.Getenv("MINISKY_PROJECT_ID")); value != "" {
		return value
	}
	data, err := os.ReadFile(cliProjectConfigPath())
	if err == nil {
		var saved cliProjectConfig
		if json.Unmarshal(data, &saved) == nil && strings.TrimSpace(saved.DefaultProject) != "" {
			return saved.DefaultProject
		}
	}
	return defaultProject
}

func cliProjectConfigPath() string {
	return filepath.Join(config.GetProfileDir(), "cli.json")
}

func saveCLIProjectConfig(value cliProjectConfig) error {
	if strings.TrimSpace(value.DefaultProject) == "" {
		return errors.New("default project is required")
	}
	path := cliProjectConfigPath()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	payload, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	temp, err := os.CreateTemp(filepath.Dir(path), ".cli-config-*")
	if err != nil {
		return err
	}
	name := temp.Name()
	defer os.Remove(name)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return err
	}
	if _, err := temp.Write(payload); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	return nil
}

func init() {
	rootCmd.AddCommand(projectCmd)
}
