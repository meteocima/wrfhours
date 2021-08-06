package main

import (
	"flag"
	"fmt"
)

// Version of the command
var Version string = "development"

func main() {
	showver := flag.Bool("v", false, "print version to stdout")
	flag.Parse()
	if showver != nil && *showver {
		fmt.Printf("wrfhours ver. %s\n", Version)
		return
	}
}
