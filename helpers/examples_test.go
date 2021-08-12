package helpers_test

import (
	"embed"
	"fmt"
	"io/fs"

	"github.com/meteocima/wrfhours/helpers"
)

//go:embed fixtures
var fixtureRootFS embed.FS
var fixtureFS, _ = fs.Sub(fixtureRootFS, "fixtures")

// This example parse an existing WRF log file,
// and print the first two output files found there.
func ExampleParseFile() {
	parser, err := helpers.ParseFile(fixtureFS, "rsl.out.0000")
	if err != nil {
		panic(err)
	}
	i := 0
	for f := range parser.Files {
		fmt.Println(f.HourProgr, f.Type, f.Instant)
		i++
		if i == 2 {
			break
		}
		if f.Err != nil {
			panic(f.Err)
		}
	}

	// Output: 0 wrfout 2021-08-04 00:00:00 +0000 UTC
	// 0 auxhist2 2021-08-04 00:00:00 +0000 UTC
}
