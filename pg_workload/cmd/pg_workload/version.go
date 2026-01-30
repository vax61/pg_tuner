package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

const Version = "0.1.0-dev"

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version information",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("pg_workload version %s\n", Version)
	},
}
