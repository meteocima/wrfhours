package json

import (
	"embed"
	"fmt"
	"io"
	"io/fs"
	"testing"
	"time"

	"github.com/meteocima/wrfhours"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//go:embed fixtures
var fixtureRootFS embed.FS
var fixtureFS, _ = fs.Sub(fixtureRootFS, "fixtures")

func TestParseFile(t *testing.T) {

	t.Run("Unmarshal on wrong JSON", func(t *testing.T) {

		r, w := io.Pipe()

		go func() {
			defer w.Close()
			fmt.Fprintf(w, "TEST\n")
		}()

		results := UnmarshalResultsStream(r)
		require.NotNil(t, results)
		f := <-results.Files
		require.NotNil(t, f)

		assert.EqualError(t, f.Err, "UnmarshalResultsStream failed: error while reading: invalid character 'T' looking for beginning of value")

	})

	t.Run("Marshal / Unmarshal", func(t *testing.T) {

		file, err := fixtureFS.Open("rsl.out.0000")
		require.NoError(t, err)
		defer file.Close()

		r, w := io.Pipe()

		go func() {
			defer w.Close()
			err := MarshalStreams(file, w)
			require.NoError(t, err)
		}()

		results := UnmarshalResultsStream(r)

		actual, err := results.Collect()
		require.NoError(t, err)
		checkResults(t, actual)

	})

	t.Run("Marshal on failing writer", func(t *testing.T) {

		file, err := fixtureFS.Open("rsl.out.0000")
		require.NoError(t, err)
		defer file.Close()

		w := failingWriter{}

		err = MarshalStreams(file, w)
		assert.EqualError(t, err, "MarshalStreams failed: error while writing: TEST")

	})

}

type failingWriter struct{}

func (w failingWriter) Write(p []byte) (n int, err error) {
	return 0, fmt.Errorf("TEST")
}

func checkResults(t *testing.T, actual []wrfhours.FileInfo) {
	assert.Equal(t, 201, len(actual))

	assert.Equal(t, wrfhours.FileInfo{
		Type:      "wrfout",
		Domain:    1,
		Instant:   time.Date(2021, 8, 4, 0, 0, 0, 0, time.UTC),
		Filename:  "wrfout_d01_2021-08-04_00:00:00",
		HourProgr: 0,
	}, actual[0])

	assert.Equal(t, wrfhours.FileInfo{
		Type:      "wrfout",
		Domain:    3,
		Instant:   time.Date(2021, 8, 4, 1, 0, 0, 0, time.UTC),
		Filename:  "wrfout_d03_2021-08-04_01:00:00",
		HourProgr: 1,
	}, actual[10])

	assert.Equal(t, wrfhours.FileInfo{
		Type:      "auxhist23",
		Domain:    3,
		Instant:   time.Date(2021, 8, 5, 23, 0, 0, 0, time.UTC),
		Filename:  "auxhist23_d03_2021-08-05_23:00:00",
		HourProgr: 47,
	}, actual[196])
}
