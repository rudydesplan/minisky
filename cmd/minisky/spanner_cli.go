package main

import (
	"github.com/spf13/cobra"
)

var spannerCmd = &cobra.Command{
	Use:   "spanner",
	Short: "Manage Spanner resources",
}

var spannerProject string

var spannerInstancesCmd = &cobra.Command{
	Use:   "instances",
	Short: "Manage Spanner instances",
}

var listSpannerInstancesCmd = &cobra.Command{
	Use:   "list",
	Short: "List Spanner instances",
	Run: func(cmd *cobra.Command, args []string) {
		unsupportedHeadlessCommand(cmd, "Spanner")
	},
}

var createSpannerInstanceCmd = &cobra.Command{
	Use:   "create [instance-id]",
	Short: "Create a Spanner instance",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		unsupportedHeadlessCommand(cmd, "Spanner")
	},
}

func init() {
	spannerCmd.PersistentFlags().StringVar(&spannerProject, "project", "local-dev-project", "GCP Project ID")
	spannerInstancesCmd.AddCommand(listSpannerInstancesCmd)
	spannerInstancesCmd.AddCommand(createSpannerInstanceCmd)
	spannerCmd.AddCommand(spannerInstancesCmd)
	rootCmd.AddCommand(spannerCmd)
}
