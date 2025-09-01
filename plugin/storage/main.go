package main

import (
	"fmt"
	"os"

	sdk "github.com/PlakarKorp/go-kloset-sdk"
	"github.com/PlakarKorp/integration-http/storage"
)

func main() {
	if len(os.Args) != 1 {
		fmt.Fprintf(os.Stderr, "Usage: %s\n", os.Args[0])
		os.Exit(1)
	}

	if err := sdk.RunStorage(storage.NewStore); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to initialize the SDK:", err)
		os.Exit(1)
	}
}
