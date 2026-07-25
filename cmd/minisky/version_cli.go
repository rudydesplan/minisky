package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"minisky/pkg/version"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of MiniSky",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("MiniSky v%s\n", version.Version)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
