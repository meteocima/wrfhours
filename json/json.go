package json

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/meteocima/wrfhours"
)

// Marshal ...
func Marshal(in io.Reader, out io.Writer, timeout time.Duration) error {
	parser := wrfhours.NewParser(timeout)

	go parser.Parse(in)

	for file := range parser.Files {
		if file.Err != nil {
			return file.Err
		}
		buff, err := json.Marshal(file)
		if err != nil {
			return err
		}

		if _, err = fmt.Fprintln(out, string(buff)); err != nil {
			return fmt.Errorf("Marshal failed: error while writing: %w", err)
		}
	}

	fmt.Println("MARSHAL DONE")

	return nil
}

// Unmarshal parse results of wrfoutput command
// and unmarshal it into a channel of FileInfo structs
func Unmarshal(r io.Reader) *wrfhours.Parser {
	results := wrfhours.NewParser(time.Second)

	go func() {
		var err error

		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			line := scanner.Bytes()
			var file wrfhours.FileInfo
			// fmt.Printlnln("unmarshal")
			err = json.Unmarshal(line, &file)
			if err != nil {
				// fmt.Printlnln("err found")
				break
			}
			results.EmitFile(file)
		}
		if err == nil {
			// fmt.Printlnln("err nil, check scanner")
			err = scanner.Err()
		}

		if err != nil {
			// fmt.Printlnln("err!")
			err = fmt.Errorf("Unmarshal failed: error while reading: %w", err)
			// fmt.Printlnln(err)
			results.EmitError(err)
			return
		}
		// // fmt.Printlnln("err nil close files")
		results.Close()
	}()

	return results
}
