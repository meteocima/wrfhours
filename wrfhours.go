package wrfhours

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"
)

const filesPrefix = "Timing for Writing "

// FileInfo contains information about a single file
// created by WRF.
type FileInfo struct {
	// type of file, e.g. auxhist23, wrfout etc.
	Type    string
	Domain  int
	Instant time.Time
	// Progressive number of hour starting from the
	// first hour of the simulation
	// (0 based, start of the simulation
	// is hour 0)
	HourProgr int
	Filename  string
	Err       error
}

type execHandler struct {
	fn     func(info *FileInfo) error
	filter Filter
}

// Filter contains filter to
// tell wich file to filter
// in a OnFileDo execution
type Filter struct {
	// type of file to filter, e.g. auxhist23, wrfout etc.
	// if an empty string, no filter is applyed for file type
	Type string
	// domain to filter, if 0 , no filter is applyed for domain
	Domain int
}

// All ...
var All = Filter{}

var restartFile *FileInfo = nil

func noop() error {
	return nil
}

// ParseFile parse WRF log from a given file.
func ParseFile(wrfLogPath string) *Parser {

	file, err := os.Open(wrfLogPath)
	if err != nil {
		parser := NewParser(time.Millisecond)
		go parser.EmitError(err)
		return &parser
	}

	res := Parse(file, 100*time.Millisecond)
	res.SetOnClose(file.Close)

	return res
}

// Parse parse WRF log from a given file.
func Parse(r io.Reader, timeout time.Duration) *Parser {
	parser := NewParser(timeout)

	go parser.Parse(r)

	return &parser
}

// MarshalStreams ...
func MarshalStreams(in io.Reader, out io.Writer) error {
	parser := NewParser(10 * time.Millisecond)

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
			return fmt.Errorf("MarshalStreams failed: error while writing: %w", err)
		}
	}

	return nil
}

// UnmarshalResultsStream parse results of wrfoutput command
// and unmarshal it into a channel of FileInfo structs
func UnmarshalResultsStream(r io.Reader) *Parser {
	results := NewParser(time.Second)

	go func() {
		var err error

		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			line := scanner.Bytes()
			var file FileInfo
			err = json.Unmarshal(line, &file)
			if err != nil {
				break
			}
			results.files <- &file
		}
		if err == nil {
			err = scanner.Err()
		}

		if err != nil {
			err = fmt.Errorf("UnmarshalResultsStream failed: error while reading: %w", err)
			results.EmitError(err)
			return
		}
		close(results.files)
	}()

	return &results
}
