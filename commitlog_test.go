package commitlog_test

import (
        "io/ioutil"
        "os"
        "testing"

        "github.com/HoMuChen/commitlog"
)

func TestNew(t *testing.T) {
        cl, err := commitlog.New("test.db", nil)

        if err != nil {
                t.Error(err)
        }

        offset, err := cl.Append([]byte(`I am the first message`))
        if err != nil {
                t.Error(err)
        }
        if offset != 0 {
                t.Error(offset)
        }

        cl.Append([]byte(`123`))
        cl.Append([]byte(`123`))
        cl.Append([]byte(`123`))
        cl.Append([]byte(`123`))

        offset = cl.Offset()
        if offset != 4 {
                t.Errorf("Append five times. Expect offset: 4, but got: %v", offset)
        }

        cleanup(cl)
}

func TestNewSegment(t *testing.T) {
        cl, err := commitlog.New("test.db", &commitlog.Options{20}) //20 bytes max segment size

        if err != nil {
                t.Error(err)
        }

        cl.Append([]byte(`0123456789`)) //10 bytes
        cl.Append([]byte(`0123456789`)) //10 bytes
        cl.Append([]byte(`0123456789`)) //10 bytes, open another new segment

        total := cl.Offset()

        if total != 2 {
                t.Errorf("Append three times. Expect offset: 2, but got: %v", total)
        }

        files, _ := ioutil.ReadDir("test.db")

        if len(files) != 4 {
                t.Errorf("Expect 4 files 0.log 0.index 2.log 2.index, but got %v", len(files))
        }

        cleanup(cl)
}

func cleanup(cl *commitlog.CommitLog) {
	os.RemoveAll(cl.Path)
}
