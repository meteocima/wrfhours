package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/meteocima/wrfoutput"
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

	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func run() error {
	parser := wrfoutput.NewParser()

	go parser.Parse(os.Stdin)

	for file := range parser.Results.Files {
		buff, err := json.Marshal(file)
		if err != nil {
			return err
		}

		if _, err = fmt.Fprintln(os.Stdout, string(buff)); err != nil {
			return err
		}
	}

	if err := <-parser.Results.Errs; err != nil {
		return err
	}

	return nil
}
