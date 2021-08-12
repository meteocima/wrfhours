package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/meteocima/wrfhours/json"
)

// Version of the command
var Version string = "development"

func main() {
	showver := flag.Bool("v", false, "print version to stdout")
	timeout := flag.Int64("t", 1, "timeout in seconds")
	flag.Parse()
	if showver != nil && *showver {
		fmt.Printf("wrfhours ver. %s\n", Version)
		os.Exit(0)
	}

	if err := json.Marshal(os.Stdin, os.Stdout, time.Duration(*timeout)*time.Second); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

}
