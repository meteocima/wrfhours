package wrfoutput

import (
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fixtures(file string) string {
	_, thisfile, _, ok := runtime.Caller(0)
	if !ok {
		panic("cannot retrieve the source file path")
	}

	rootdir := filepath.Dir(thisfile)

	return path.Join(rootdir, "fixtures", file)
}

func TestParseFile(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	t.Run("emit error on failed on close", func(t *testing.T) {
		r := strings.NewReader(`
d01 2021-08-04_00:00:00  alloc_space_field: domain            2 ,                5403068  bytes allocated
Timing for Writing auxhist23_d01_2021-08-06_00:00:00 for domain        1:    0.10153 elapsed seconds
		`)

		results := Parse(r)
		results.OnClose = func() error {
			return errors.New("TEST")
		}
		actual, err := results.Collect()
		assert.Nil(actual)
		assert.EqualError(err, "OnClose hook failed: TEST")
	})

	//return

	t.Run("emit error on wrong domain", func(t *testing.T) {
		results := ParseFile(fixtures("wrong-domain"))
		actual, err := results.Collect()
		assert.Nil(actual)
		assert.EqualError(err, "Wrong format for timing line `Timing for Writing auxhist23_d01_2021-08-06_00:00:00 for!!domain        1:    0.10153 elapsed seconds`: `for domain` expected to appears in line")
	})

	t.Run("silly test", func(t *testing.T) {
		noop()
		assert.Nil(nil)
	})

	t.Run("emit error when start instant is missing", func(t *testing.T) {
		results := ParseFile(fixtures("wrong-without-start-instant"))
		actual, err := results.Collect()
		assert.Nil(actual)
		assert.EqualError(err, "Start line not found yet")
	})

	t.Run("emit error on file open error", func(t *testing.T) {
		results := ParseFile("doesnt-exist")
		actual, err := results.Collect()
		assert.Nil(actual)
		assert.EqualError(err, "open doesnt-exist: no such file or directory")
	})

	t.Run("emit error on wrong number of filename parts", func(t *testing.T) {
		results := ParseFile(fixtures("wrong-filename-parts"))
		actual, err := results.Collect()
		assert.Nil(actual)
		assert.EqualError(err, "Wrong format for timing line `Timing for Writing auxhist23_d01_2021-08-06_00_00:00 for domain        1:    0.10153 elapsed seconds`: filename expected to be formed by 4 parts separated by underscores")
	})

	t.Run("emit error on wrong domain number", func(t *testing.T) {
		results := ParseFile(fixtures("wrong-domain-num"))
		actual, err := results.Collect()
		assert.Nil(actual)
		assert.EqualError(err, "Wrong format for timing line `Timing for Writing auxhist23_dF1_2021-08-06_00:00:00 for domain        1:    0.10153 elapsed seconds`: invalid domain: strconv.ParseInt: parsing \"F1\": invalid syntax")
	})

	t.Run("emit error on wrong instant", func(t *testing.T) {
		results := ParseFile(fixtures("wrong-instant"))
		actual, err := results.Collect()
		assert.Nil(actual)
		assert.EqualError(err, "Wrong format for timing line `Timing for Writing auxhist23_d01_2021-08-RR_00:00:00 for domain        1:    0.10153 elapsed seconds`: invalid time instant: parsing time \"2021-08-RR00:00:00\" as \"2006-01-0215:04:05\": cannot parse \"RR00:00:00\" as \"02\"")
	})

	t.Run("emit error on wrong start instant line", func(t *testing.T) {
		results := ParseFile(fixtures("wrong-start-instant"))
		actual, err := results.Collect()
		assert.Nil(actual)
		assert.EqualError(err, "Wrong format for start instant line `d01 2021-08-04_00:00:00`: line must contains at leas 3 space separated parts. e.g. `d01 2021-08-04_00:00:00 something`")
	})

	t.Run("emit error on wrong start instant date format", func(t *testing.T) {
		results := ParseFile(fixtures("wrong-start-instant-format"))
		actual, err := results.Collect()
		assert.Nil(actual)
		assert.EqualError(err, "Wrong format for start instant line `d01 2021-08-RR_00:00:00 ciao`: parsing time \"2021-08-RR_00:00:00\" as \"2006-01-02_15:04:05\": cannot parse \"RR_00:00:00\" as \"02\"")
	})

	checkResults := func(actual []*FileInfo) {
		assert.Equal(201, len(actual))

		assert.Equal(FileInfo{
			Type:      "wrfout",
			Domain:    1,
			Instant:   time.Date(2021, 8, 4, 0, 0, 0, 0, time.UTC),
			Filename:  "wrfout_d01_2021-08-04_00:00:00",
			HourProgr: 0,
		}, *actual[0])

		assert.Equal(FileInfo{
			Type:      "wrfout",
			Domain:    3,
			Instant:   time.Date(2021, 8, 4, 1, 0, 0, 0, time.UTC),
			Filename:  "wrfout_d03_2021-08-04_01:00:00",
			HourProgr: 1,
		}, *actual[10])

		assert.Equal(FileInfo{
			Type:      "auxhist23",
			Domain:    3,
			Instant:   time.Date(2021, 8, 5, 23, 0, 0, 0, time.UTC),
			Filename:  "auxhist23_d03_2021-08-05_23:00:00",
			HourProgr: 47,
		}, *actual[196])
	}

	t.Run("Marshal / Unmarshal", func(t *testing.T) {
		expected := make([]int, 50)

		for idx := 0; idx <= 49; idx++ {
			expected[idx] = idx
		}

		file, err := os.Open(fixtures("rsl.out.0000"))
		require.NoError(err)
		defer file.Close()

		r, w := io.Pipe()

		go func() {
			defer w.Close()
			err := MarshalStreams(file, w)
			require.NoError(err)
		}()

		results := UnmarshalResultsStream(r)

		actual, err := results.Collect()
		require.NoError(err)
		checkResults(actual)

	})

	t.Run("EachFileDo complete file", func(t *testing.T) {
		expected := make([]int, 50)

		for idx := 0; idx <= 49; idx++ {
			expected[idx] = idx
		}

		results := ParseFile(fixtures("rsl.out.0000"))
		var actual []*FileInfo

		err := results.EachFileDo(func(file *FileInfo) error {
			actual = append(actual, file)
			return nil
		})

		require.NoError(err)

		checkResults(actual)
	})

	t.Run("Collect complete file", func(t *testing.T) {
		expected := make([]int, 50)

		for idx := 0; idx <= 49; idx++ {
			expected[idx] = idx
		}

		results := ParseFile(fixtures("rsl.out.0000"))
		actual, err := results.Collect()
		require.NoError(err)

		checkResults(actual)

	})

}
