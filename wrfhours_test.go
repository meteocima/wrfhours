package wrfhours

import (
	"embed"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"testing"
	"time"

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
		f := <-results.files
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

	t.Run("emit error on file open error", func(t *testing.T) {
		results := ParseFile(fixtureFS, "doesnt-exist")
		actual, err := results.Collect()
		assert.Nil(t, actual)
		assert.EqualError(t, err, "open doesnt-exist: file does not exist")
	})

	const successLine = "SUCCESS COMPLETE WRF"
	t.Run("emit error on timeout expired", func(t *testing.T) {
		r, w := io.Pipe()

		go func() {
			time.Sleep(10 * time.Millisecond)
			fmt.Fprintln(w, "d01 2021-08-04_00:00:00  alloc_space_field: domain            2 ,                5403068  bytes allocated")
			time.Sleep(10 * time.Millisecond)
			fmt.Fprintln(w, "Timing for Writing auxhist23_d01_2021-08-06_00:00:00 for domain        1:    0.10153 elapsed seconds")
			time.Sleep(140 * time.Millisecond)
			fmt.Fprintln(w, successLine)
			w.Close()
		}()

		results := Parse(r, 20*time.Millisecond)
		//results.SetTimeout(20 * time.Millisecond)
		actual, err := results.Collect()

		assert.Nil(t, actual)
		assert.EqualError(t, err, "Timeout expired: no new files created for more than 20ms")
	})
	t.Run("OnFileDo with multiple filters", func(t *testing.T) {

		results := ParseFile(fixtureFS, "rsl.out.0000")

		var actualD3 []*FileInfo
		var actualD1 []*FileInfo

		results.OnFileDo(Filter{Type: "wrfout", Domain: 3}, func(file *FileInfo) error {
			actualD3 = append(actualD3, file)
			return nil
		})

		results.OnFileDo(Filter{Type: "wrfout", Domain: 1}, func(file *FileInfo) error {
			actualD1 = append(actualD1, file)
			return nil
		})

		require.NoError(t, results.Execute())

		assert.Equal(t, 1, len(actualD1))

		assert.Equal(t, FileInfo{
			Type:      "wrfout",
			Domain:    1,
			Instant:   time.Date(2021, 8, 4, 0, 0, 0, 0, time.UTC),
			Filename:  "wrfout_d01_2021-08-04_00:00:00",
			HourProgr: 0,
		}, *actualD1[0])

		assert.Equal(t, 49, len(actualD3))

		assert.Equal(t, FileInfo{
			Type:      "wrfout",
			Domain:    3,
			Instant:   time.Date(2021, 8, 4, 0, 0, 0, 0, time.UTC),
			Filename:  "wrfout_d03_2021-08-04_00:00:00",
			HourProgr: 0,
		}, *actualD3[0])

		//Timing for Writing wrfout_d03_2021-08-04_08:00:00 for domain        3:    0.88979 elapsed seconds

		assert.Equal(t, FileInfo{
			Type:      "wrfout",
			Domain:    3,
			Instant:   time.Date(2021, 8, 4, 10, 0, 0, 0, time.UTC),
			Filename:  "wrfout_d03_2021-08-04_10:00:00",
			HourProgr: 10,
		}, *actualD3[10])

	})

	t.Run("emit error on no success line", func(t *testing.T) {
		r, w := io.Pipe()

		go func() {
			fmt.Fprintln(w, "d01 2021-08-04_00:00:00  alloc_space_field: domain            2 ,                5403068  bytes allocated")
			fmt.Fprintln(w, "Timing for Writing auxhist23_d01_2021-08-06_00:00:00 for domain        1:    0.10153 elapsed seconds")
			w.Close()
		}()

		results := Parse(r, 30*time.Millisecond)
		actual, err := results.Collect()

		assert.Nil(t, actual)
		assert.EqualError(t, err, "input stream completed without success log line")
	})

	t.Run("parse stream with pauses", func(t *testing.T) {
		r, w := io.Pipe()

		go func() {
			time.Sleep(10 * time.Millisecond)
			fmt.Fprintln(w, "d01 2021-08-04_00:00:00  alloc_space_field: domain            2 ,                5403068  bytes allocated")
			time.Sleep(10 * time.Millisecond)
			fmt.Fprintln(w, "Timing for Writing auxhist23_d01_2021-08-06_00:00:00 for domain        1:    0.10153 elapsed seconds")
			time.Sleep(10 * time.Millisecond)
			fmt.Fprintln(w, successLine)
			w.Close()
		}()

		results := Parse(r, 30*time.Millisecond)
		actual, err := results.Collect()

		require.NoError(t, err)

		assert.Equal(t, 1, len(actual))

		assert.Equal(t, FileInfo{
			Type:      "auxhist23",
			Domain:    1,
			Instant:   time.Date(2021, 8, 6, 0, 0, 0, 0, time.UTC),
			Filename:  "auxhist23_d01_2021-08-06_00:00:00",
			HourProgr: 48,
		}, *actual[0])
	})

	t.Run("emit error on failed on close", func(t *testing.T) {
		r := strings.NewReader(`
d01 2021-08-04_00:00:00  alloc_space_field: domain            2 ,                5403068  bytes allocated
Timing for Writing auxhist23_d01_2021-08-06_00:00:00 for domain        1:    0.10153 elapsed seconds
SUCCESS COMPLETE WRF
		`)

		results := Parse(r, 20*time.Millisecond)
		results.SetOnClose(func() error {
			return errors.New("TEST")
		})
		actual, err := results.Collect()
		assert.Nil(t, actual)
		assert.EqualError(t, err, "OnClose hook failed: TEST")
	})

	t.Run("emit error on wrong domain", func(t *testing.T) {
		results := ParseFile(fixtureFS, "wrong-domain")
		actual, err := results.Collect()
		assert.Nil(t, actual)
		assert.EqualError(t, err, "Wrong format for timing line `Timing for Writing auxhist23_d01_2021-08-06_00:00:00 for!!domain        1:    0.10153 elapsed seconds`: `for domain` expected to appears in line")
	})

	t.Run("silly test", func(t *testing.T) {
		noop()
		assert.Nil(t, nil)
	})

	t.Run("emit error when start instant is missing", func(t *testing.T) {
		results := ParseFile(fixtureFS, "wrong-without-start-instant")
		actual, err := results.Collect()
		assert.Nil(t, actual)
		assert.EqualError(t, err, "Start line not found yet")
	})

	t.Run("emit error on wrong number of filename parts", func(t *testing.T) {
		results := ParseFile(fixtureFS, "wrong-filename-parts")
		actual, err := results.Collect()
		assert.Nil(t, actual)
		assert.EqualError(t, err, "Wrong format for timing line `Timing for Writing auxhist23_d01_2021-08-06_00_00:00 for domain        1:    0.10153 elapsed seconds`: filename expected to be formed by 4 parts separated by underscores")
	})

	t.Run("emit error on wrong domain number", func(t *testing.T) {
		results := ParseFile(fixtureFS, "wrong-domain-num")
		actual, err := results.Collect()
		assert.Nil(t, actual)
		assert.EqualError(t, err, "Wrong format for timing line `Timing for Writing auxhist23_dF1_2021-08-06_00:00:00 for domain        1:    0.10153 elapsed seconds`: invalid domain: strconv.ParseInt: parsing \"F1\": invalid syntax")
	})

	t.Run("emit error on wrong instant", func(t *testing.T) {
		results := ParseFile(fixtureFS, "wrong-instant")
		actual, err := results.Collect()
		assert.Nil(t, actual)
		assert.EqualError(t, err, "Wrong format for timing line `Timing for Writing auxhist23_d01_2021-08-RR_00:00:00 for domain        1:    0.10153 elapsed seconds`: invalid time instant: parsing time \"2021-08-RR00:00:00\" as \"2006-01-0215:04:05\": cannot parse \"RR00:00:00\" as \"02\"")
	})

	t.Run("emit error on wrong start instant line", func(t *testing.T) {
		results := ParseFile(fixtureFS, "wrong-start-instant")
		actual, err := results.Collect()
		assert.Nil(t, actual)
		assert.EqualError(t, err, "Wrong format for start instant line `d01 2021-08-04_00:00:00`: line must contains at leas 3 space separated parts. e.g. `d01 2021-08-04_00:00:00 something`")
	})

	t.Run("emit error on wrong start instant date format", func(t *testing.T) {
		results := ParseFile(fixtureFS, "wrong-start-instant-format")
		actual, err := results.Collect()
		assert.Nil(t, actual)
		assert.EqualError(t, err, "Wrong format for start instant line `d01 2021-08-RR_00:00:00 ciao`: parsing time \"2021-08-RR_00:00:00\" as \"2006-01-02_15:04:05\": cannot parse \"RR_00:00:00\" as \"02\"")
	})

	t.Run("Marshal on failing writer", func(t *testing.T) {

		file, err := fixtureFS.Open("rsl.out.0000")
		require.NoError(t, err)
		defer file.Close()

		w := failingWriter{}

		err = MarshalStreams(file, w)
		assert.EqualError(t, err, "MarshalStreams failed: error while writing: TEST")

	})

	t.Run("OnFileDo with failing handler", func(t *testing.T) {

		results := ParseFile(fixtureFS, "rsl.out.0000")

		err := results.OnFileDo(All, func(file *FileInfo) error {
			return fmt.Errorf("TEST")
		}).Execute()

		assert.EqualError(t, err, "OnFileDo handler failed: TEST")

	})

	t.Run("OnFileDo complete file", func(t *testing.T) {

		results := ParseFile(fixtureFS, "rsl.out.0000")
		var actual []*FileInfo

		err := results.OnFileDo(All, func(file *FileInfo) error {
			actual = append(actual, file)
			return nil
		}).Execute()

		require.NoError(t, err)

		checkResults(t, actual)
	})

	t.Run("OnFileDo with filters", func(t *testing.T) {

		results := ParseFile(fixtureFS, "rsl.out.0000")
		var actual []*FileInfo

		err := results.OnFileDo(Filter{Type: "wrfout", Domain: 3}, func(file *FileInfo) error {
			actual = append(actual, file)
			return nil
		}).Execute()

		require.NoError(t, err)

		assert.Equal(t, 49, len(actual))

		assert.Equal(t, FileInfo{
			Type:      "wrfout",
			Domain:    3,
			Instant:   time.Date(2021, 8, 4, 0, 0, 0, 0, time.UTC),
			Filename:  "wrfout_d03_2021-08-04_00:00:00",
			HourProgr: 0,
		}, *actual[0])

		//Timing for Writing wrfout_d03_2021-08-04_08:00:00 for domain        3:    0.88979 elapsed seconds

		assert.Equal(t, FileInfo{
			Type:      "wrfout",
			Domain:    3,
			Instant:   time.Date(2021, 8, 4, 10, 0, 0, 0, time.UTC),
			Filename:  "wrfout_d03_2021-08-04_10:00:00",
			HourProgr: 10,
		}, *actual[10])

	})

	t.Run("Collect complete file", func(t *testing.T) {

		results := ParseFile(fixtureFS, "rsl.out.0000")
		actual, err := results.Collect()
		require.NoError(t, err)

		checkResults(t, actual)

	})

}

type failingWriter struct{}

func (w failingWriter) Write(p []byte) (n int, err error) {
	return 0, fmt.Errorf("TEST")
}

func checkResults(t *testing.T, actual []*FileInfo) {
	assert.Equal(t, 201, len(actual))

	assert.Equal(t, FileInfo{
		Type:      "wrfout",
		Domain:    1,
		Instant:   time.Date(2021, 8, 4, 0, 0, 0, 0, time.UTC),
		Filename:  "wrfout_d01_2021-08-04_00:00:00",
		HourProgr: 0,
	}, *actual[0])

	assert.Equal(t, FileInfo{
		Type:      "wrfout",
		Domain:    3,
		Instant:   time.Date(2021, 8, 4, 1, 0, 0, 0, time.UTC),
		Filename:  "wrfout_d03_2021-08-04_01:00:00",
		HourProgr: 1,
	}, *actual[10])

	assert.Equal(t, FileInfo{
		Type:      "auxhist23",
		Domain:    3,
		Instant:   time.Date(2021, 8, 5, 23, 0, 0, 0, time.UTC),
		Filename:  "auxhist23_d03_2021-08-05_23:00:00",
		HourProgr: 47,
	}, *actual[196])
}
