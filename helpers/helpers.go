package helpers

import (
	"io"
	"io/fs"
	"time"

	"github.com/meteocima/wrfhours"
)

// ParseFile parse WRF log from a given file.
func ParseFile(fs fs.FS, wrfLogPath string) (*wrfhours.Parser, error) {

	file, err := fs.Open(wrfLogPath)
	if err != nil {
		return nil, err
	}

	res := Parse(file, 100*time.Millisecond)
	res.SetOnClose(file.Close)

	return res, nil
}

// Parse parse WRF log from a given file.
func Parse(r io.Reader, timeout time.Duration) *wrfhours.Parser {
	parser := wrfhours.NewParser(timeout)

	go parser.Parse(r)

	return &parser
}
