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

// Parser ...
type Parser struct {
	currline string
	Start    *time.Time
	Results  *Results
}

// NewParser ...
func NewParser(timeout time.Duration) Parser {

	files := make(chan *FileInfo)

	return Parser{
		Results: &Results{
			Files:   NewFileInfoChan(timeout, files),
			onClose: noop,
			files:   files,
		},
	}
}

func (parser *Parser) runOnClose() {
	parser.Results.lock.Lock()
	defer parser.Results.lock.Unlock()

	if err := parser.Results.onClose(); err != nil {
		parser.Results.EmitError(fmt.Errorf("OnClose hook failed: %w", err))
		return
	}

	close(parser.Results.files)
}

// Parse ...
func (parser *Parser) Parse(r io.Reader) {

	scanner := bufio.NewScanner(r)

	for scanner.Scan() /**&& !hasDone*/ {
		parser.currline = scanner.Text()
		if err := parser.parseCurrLine(); err != nil {
			if err.Error() == "completed" {
				parser.runOnClose()
				return
			}
			parser.Results.EmitError(err)
			return
		}
	}

	if err := scanner.Err(); err != nil {
		parser.Results.EmitError(err)
		return
	}

	err := fmt.Errorf("input stream completed without success log line")
	parser.Results.EmitError(err)

	parser.Results.lock.Lock()
	defer parser.Results.lock.Unlock()

	parser.Results.onClose()

}

func (parser *Parser) parseCurrLine() error {

	if parser.isStartInstantLine() {
		if err := parser.parseStartInstant(); err != nil {
			return err
		}
		return nil
	}

	if parser.isFileInfoLine() {
		info, err := parser.parseFileInfo()
		if err != nil {

			return err
		}

		if info != restartFile {
			parser.Results.files <- info
		}
	}

	if parser.isSuccessLine() {
		return fmt.Errorf("completed")
	}

	return nil

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

func (parser *Parser) isSuccessLine() bool {
	return strings.HasSuffix(parser.currline, "SUCCESS COMPLETE WRF")
}

func (parser *Parser) isStartInstantLine() bool {
	return strings.HasPrefix(parser.currline, "d01 ") && parser.Start == nil
}

func (parser *Parser) isFileInfoLine() bool {
	return strings.HasPrefix(parser.currline, filesPrefix)
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
	Files FileInfoChan
	files chan *FileInfo
	//Errs     chan error
	onClose  func() error
	lock     sync.Mutex
	handlers []execHandler
	timeout  time.Duration
}

// EmitError ...
func (r *Results) EmitError(err error) {
	r.files <- &FileInfo{Err: err}
	close(r.files)
}

// SetOnClose ...
func (r *Results) SetOnClose(fn func() error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.onClose = fn
}

// Collect ...
func (r *Results) Collect() ([]*FileInfo, error) {
	actual := []*FileInfo{}

	for file := range r.Files {
		if file.Err != nil {
			return nil, file.Err
		}
		actual = append(actual, file)
	}

	return actual, nil
}

// Execute ...
func (r *Results) Execute() error {
	for file := range r.Files {
		if file.Err != nil {
			return file.Err
		}
		for _, handler := range r.handlers {
			if handler.filter.Domain != 0 && handler.filter.Domain != file.Domain {
				continue
			}
			if handler.filter.Type != "" && handler.filter.Type != file.Type {
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
func (r *Results) OnFileDo(filter Filter, fn func(info *FileInfo) error) *Results {
	r.handlers = append(r.handlers, execHandler{fn, filter})
	return r
}
