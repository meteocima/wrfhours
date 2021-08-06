package wrfoutput

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
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
}

// Results contains the results of a
// Parse method call. It consists of
// an Errs chan, eventually emitting
// a single error when one occurs; a
// Files chan, which emit all files info
// parsed; a StartInstant, containing
// first time instant of the simulation.
// Files channel is blocking, and should be
// read by the caller in order for the parsing
// to proceed. Errs has a buffer of 1,
// so it could be checked for errors after
// the caller has done reading Files channel.
// Both channel are closed by the Parse call.
type Results struct {
	Files   chan *FileInfo
	Errs    chan error
	OnClose func() error
}

// Collect ...
func (r Results) Collect() ([]*FileInfo, error) {
	var actual []*FileInfo

	for file := range r.Files {
		actual = append(actual, file)
	}

	err := <-r.Errs
	if err != nil {
		return nil, err
	}
	return actual, nil
}

// EachFileDo ...
func (r Results) EachFileDo(fn func(info *FileInfo) error) error {
	for file := range r.Files {
		err := fn(file)
		if err != nil {
			return err
		}
	}

	return <-r.Errs
}

func (r Results) close(prevErr *error) {
	close(r.Files)
	if *prevErr == nil {
		if err := r.OnClose(); err != nil {
			*prevErr = fmt.Errorf("OnClose hook failed: %w", err)
		}
	}
	if *prevErr != nil {
		r.Errs <- *prevErr
	}
	close(r.Errs)
}

var restartFile *FileInfo = nil

func noop() error {
	return nil
}

// ParseFile parse WRF log from a given file.
func ParseFile(wrfLogPath string) *Results {

	file, err := os.Open(wrfLogPath)
	if err != nil {
		parser := NewParser()
		parser.Results.close(&err)
		return parser.Results
	}

	res := Parse(file)
	res.OnClose = file.Close

	return res
}

// Parse parse WRF log from a given file.
func Parse(r io.Reader) *Results {
	parser := NewParser()

	go parser.Parse(r)

	return parser.Results
}

// Parser ...
type Parser struct {
	currline string
	Start    *time.Time
	Results  *Results
	failure  error
}

// NewParser ...
func NewParser() Parser {
	return Parser{
		Results: &Results{
			Files:   make(chan *FileInfo),
			Errs:    make(chan error, 1),
			OnClose: noop,
		},
	}
}

// Parse ...
func (parser *Parser) Parse(r io.Reader) {
	defer parser.Results.close(&parser.failure)

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		parser.currline = scanner.Text()
		if parser.parseCurrLine() {
			return
		}
	}

	parser.failure = scanner.Err()
}

func (parser *Parser) parseCurrLine() bool {

	if parser.isStartInstantLine() {
		if err := parser.parseStartInstant(); err != nil {
			parser.failure = err
			return true
		}
		return false
	}

	if parser.isFileInfoLine() {
		info, err := parser.parseFileInfo()
		if err != nil {
			parser.failure = err
			return true
		}

		if info != restartFile {
			parser.Results.Files <- info
		}
	}
	return false

}

// parse a single line already identified as a 'file writing' log line.
func (parser *Parser) parseFileInfo() (info *FileInfo, failure error) {
	if parser.Start == nil {
		return nil, fmt.Errorf("Start line not found yet")
	}

	defer func() {
		if failure != nil {
			failure = fmt.Errorf("Wrong format for timing line `%s`: %w", parser.currline, failure)
			info = nil
		}
	}()

	info = &FileInfo{}

	// line contains: Timing for Writing auxhist23_d03_2021-08-04_01:00:00 for domain        3:   10.02259 elapsed seconds
	fname := strings.TrimPrefix(parser.currline, filesPrefix)

	// fname contains: auxhist23_d03_2021-08-04_01:00:00 for domain        3:   10.02259 elapsed seconds
	fnameParts := strings.Split(fname, " for domain")
	if len(fnameParts) != 2 {
		return nil, fmt.Errorf("`for domain` expected to appears in line")
	}

	info.Filename = strings.TrimSpace(fnameParts[0])

	// skip WRF restart files with this form:
	// `Timing for Writing restart for domain        1:    1.33332 elapsed seconds`
	if info.Filename == "restart" {
		return nil, nil
	}

	// filename contains: auxhist23_d03_2021-08-04_01:00:00
	filenameParts := strings.Split(info.Filename, "_")
	if len(filenameParts) != 4 {
		return info, fmt.Errorf("filename expected to be formed by 4 parts separated by underscores")
	}

	// filenameParts[0] == auxhist23
	info.Type = filenameParts[0]

	// filenameParts[1] == d03
	trimmedDomain := strings.TrimPrefix(filenameParts[1], "d")
	if domain, err := strconv.ParseInt(trimmedDomain, 10, 32); err == nil {
		info.Domain = int(domain)
	} else {
		return nil, fmt.Errorf("invalid domain: %w", err)
	}

	// filenameParts[2]+filenameParts[3] == 2021-08-0401:00:00
	if instant, err := time.Parse("2006-01-0215:04:05", filenameParts[2]+filenameParts[3]); err == nil {
		info.Instant = instant
	} else {
		return nil, fmt.Errorf("invalid time instant: %w", err)
	}

	info.HourProgr = int(info.Instant.Sub(*parser.Start).Hours())

	//fmt.Println(info)
	return info, nil
}

func (parser *Parser) parseStartInstant() error {
	// first line starting with d01 contains first instant of simulation
	// The line appear as:
	// d01 2021-08-04_00:00:00  alloc_space_field: domain            2 ,                5403068  bytes allocated
	lineParts := strings.SplitN(parser.currline, " ", 3)
	if len(lineParts) != 3 {
		return fmt.Errorf("Wrong format for start instant line `%s`: line must contains at leas 3 space separated parts. e.g. `d01 2021-08-04_00:00:00 something`", parser.currline)

	}
	if instant, err := time.Parse("2006-01-02_15:04:05", lineParts[1]); err == nil {
		parser.Start = &instant
	} else {
		return fmt.Errorf("Wrong format for start instant line `%s`: %w", parser.currline, err)
	}

	return nil
}

func (parser *Parser) isStartInstantLine() bool {
	return strings.HasPrefix(parser.currline, "d01 ")
}

func (parser *Parser) isFileInfoLine() bool {
	return strings.HasPrefix(parser.currline, filesPrefix)
}