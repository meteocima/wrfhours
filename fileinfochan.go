package wrfoutput

import (
	"fmt"
	"time"
)

// FileInfoChan ...
type FileInfoChan chan *FileInfo

// NewFileInfoChan ...
func NewFileInfoChan(timeout time.Duration, inch chan *FileInfo) FileInfoChan {
	outch := make(FileInfoChan)

	go func() {
		defer close(outch)

		for {
			select {
			case f := <-inch:
				if f == nil {
					return
				}

				outch <- f

				if f.Err != nil {
					return
				}
			case <-time.After(timeout):
				outch <- &FileInfo{Err: fmt.Errorf("Timeout expired: no new files created for more than %s", timeout)}
				return
			}
		}

	}()

	return outch
}
