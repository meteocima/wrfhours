package wrfhours

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
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

// IsEmpty ...
func (f FileInfo) IsEmpty() bool {
	return f.Type == "" && f.Err == nil
}

// IsError ...
func (f FileInfo) IsError() bool {
	return f.Type == "" && f.Err != nil
}

type execHandler struct {
	fn           func(info FileInfo) error
	typeFilter   string
	domainFilter int
}

// Parser contains the results of a
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
// Parser ...
type Parser struct {
	currline string
	Start    *time.Time
	Files    chan FileInfo
	files    chan FileInfo
	onClose  func() error
	lock     sync.Mutex
	handlers []execHandler
}

// NewParser ...
func NewParser(timeout time.Duration) Parser {

	files := make(chan FileInfo)

	return Parser{
		Files: newFileInfoChan(timeout, files),
		files: files,
	}
}

func (parser *Parser) runOnClose(err error) {
	parser.lock.Lock()
	onClose := parser.onClose
	parser.lock.Unlock()

	if onClose != nil {
		if e := onClose(); e != nil && err == nil {
			err = fmt.Errorf("OnClose hook failed: %w", e)
		}
	}

	if err != nil {
		parser.EmitError(err)
		return
	}

	parser.Close()
}

// Parse ...
func (parser *Parser) Parse(r io.Reader) {

	scanner := bufio.NewScanner(r)
	var err error
	for scanner.Scan() /**&& !hasDone*/ {
		parser.currline = scanner.Text()
		if err = parser.parseCurrLine(); err != nil {
			if err.Error() == "completed" {
				parser.runOnClose(nil)
				return
			}
			break
		}
	}

	if e := scanner.Err(); e != nil && err == nil {
		err = e
		return
	}
	if err == nil {
		err = fmt.Errorf("input stream completed without success log line")
	}

	parser.runOnClose(err)

}

func (parser *Parser) parseCurrLine() error {

	if parser.isStartInstantLine() {
		if err := parser.parseStartInstant(); err != nil {
			return err
		}
		return nil
	}

	if parser.isFileInfoLine() {
		info := parser.parseFileInfo()
		if info.Err != nil {
			return info.Err
		}

		if info.Type != "restart" {
			parser.files <- info
		}
	}

	if parser.isSuccessLine() {
		return fmt.Errorf("completed")
	}

	return nil

}

// EmitFile ...
func (parser *Parser) EmitFile(info FileInfo) {
	parser.files <- info
}

// Close ...
func (parser *Parser) Close() {
	close(parser.files)
}

// parse a single line already identified as a 'file writing' log line.
func (parser *Parser) parseFileInfo() (info FileInfo) {
	if parser.Start == nil {
		return FileInfo{Err: fmt.Errorf("Start line not found yet")}
	}

	defer func() {
		if info.Err != nil {
			info.Err = fmt.Errorf("Wrong format for timing line `%s`: %w", parser.currline, info.Err)
		}
	}()

	info = FileInfo{}

	// line contains: Timing for Writing auxhist23_d03_2021-08-04_01:00:00 for domain        3:   10.02259 elapsed seconds
	fname := strings.TrimPrefix(parser.currline, filesPrefix)

	// fname contains: auxhist23_d03_2021-08-04_01:00:00 for domain        3:   10.02259 elapsed seconds
	fnameParts := strings.Split(fname, " for domain")
	if len(fnameParts) != 2 {
		return FileInfo{Err: fmt.Errorf("`for domain` expected to appears in line")}
	}

	info.Filename = strings.TrimSpace(fnameParts[0])

	// skip WRF restart files with this form:
	// `Timing for Writing restart for domain        1:    1.33332 elapsed seconds`
	if info.Filename == "restart" {
		return FileInfo{Type: "restart"}
	}

	// filename contains: auxhist23_d03_2021-08-04_01:00:00
	filenameParts := strings.Split(info.Filename, "_")
	if len(filenameParts) != 4 {
		return FileInfo{Err: fmt.Errorf("filename expected to be formed by 4 parts separated by underscores")}
	}

	// filenameParts[0] == auxhist23
	info.Type = filenameParts[0]

	// filenameParts[1] == d03
	trimmedDomain := strings.TrimPrefix(filenameParts[1], "d")
	if domain, err := strconv.ParseInt(trimmedDomain, 10, 32); err == nil {
		info.Domain = int(domain)
	} else {
		return FileInfo{Err: fmt.Errorf("invalid domain: %w", err)}
	}

	// filenameParts[2]+filenameParts[3] == 2021-08-0401:00:00
	if instant, err := time.Parse("2006-01-0215:04:05", filenameParts[2]+filenameParts[3]); err == nil {
		info.Instant = instant
	} else {
		return FileInfo{Err: fmt.Errorf("invalid time instant: %w", err)}
	}

	info.HourProgr = int(info.Instant.Sub(*parser.Start).Hours())

	// fmt.Printlnln(info)
	return info
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

func (parser *Parser) isSuccessLine() bool {
	return strings.HasSuffix(parser.currline, "SUCCESS COMPLETE WRF")
}

func (parser *Parser) isStartInstantLine() bool {
	return strings.HasPrefix(parser.currline, "d01 ") && parser.Start == nil
}

func (parser *Parser) isFileInfoLine() bool {
	return strings.HasPrefix(parser.currline, filesPrefix)
}

// EmitError ...
func (parser *Parser) EmitError(err error) {
	// fmt.Printlnln("write err")
	parser.files <- FileInfo{Err: err}
	// fmt.Printlnln("err written")
	parser.Close()
	// fmt.Printlnln("files closed")
}

// SetOnClose ...
func (parser *Parser) SetOnClose(fn func() error) {
	parser.lock.Lock()
	defer parser.lock.Unlock()
	parser.onClose = fn
}

// Collect ...
func (parser *Parser) Collect() ([]FileInfo, error) {
	actual := []FileInfo{}

	for file := range parser.Files {
		if file.Err != nil {
			return nil, file.Err
		}
		actual = append(actual, file)
	}

	return actual, nil
}

// Execute ...
func (parser *Parser) Execute() error {
	for file := range parser.Files {
		if file.Err != nil {
			return file.Err
		}
		for _, handler := range parser.handlers {
			if handler.domainFilter != 0 && handler.domainFilter != file.Domain {
				continue
			}
			if handler.typeFilter != "" && handler.typeFilter != file.Type {
				continue
			}

			if err := handler.fn(file); err != nil {
				return fmt.Errorf("OnFileDo handler failed: %s", err)
			}
		}
	}

	return nil
}

// OnFileDo ...
func (parser *Parser) OnFileDo(typeFilter string, domainFilter int, fn func(info FileInfo) error) *Parser {
	parser.handlers = append(parser.handlers, execHandler{fn, typeFilter, domainFilter})
	return parser
}
