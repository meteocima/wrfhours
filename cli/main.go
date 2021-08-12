package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/meteocima/wrfhours/json"
)

// Version of the command
var Version string = "development"

func main() {
	showver := flag.Bool("v", false, "print version to stdout")
	flag.Parse()
	if showver != nil && *showver {
		fmt.Printf("wrfhours ver. %s\n", Version)
		os.Exit(0)
	}

	if err := json.MarshalStreams(os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

}
