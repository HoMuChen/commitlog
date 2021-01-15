package commitlog_test

import (
        "bytes"
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

func TestRead(t *testing.T) {
        cl, err := commitlog.New("test.db", nil)
        if err != nil {
                t.Error(err)
        }

        cl.Append([]byte(`123`))
        cl.Append([]byte(`456`))
        cl.Append([]byte(`789`))

        data, err := cl.Read(1)
        if err != nil {
                t.Errorf("Expect nil error but got: %v", err)
        }
        if !bytes.Equal([]byte(`456`), data) {
                t.Errorf("Expect got back second record: %v, but got: %v", []byte(`456`), data)
        }

        cleanup(cl)
}

func TestReadLastRecord(t *testing.T) {
        cl, err := commitlog.New("test.db", nil)
        if err != nil {
                t.Error(err)
        }

        cl.Append([]byte(`123`))
        cl.Append([]byte(`456`))
        cl.Append([]byte(`789`))

        data, err := cl.Read(2)
        if err != nil {
                t.Errorf("Expect nil error but got: %v", err)
        }
        if !bytes.Equal([]byte(`789`), data) {
                t.Errorf("Expect got back last record: %v, but got: %v", []byte(`789`), data)
        }

        cleanup(cl)
}

func TestReadRecordNotExist(t *testing.T) {
        cl, err := commitlog.New("test.db", nil)
        if err != nil {
                t.Error(err)
        }

        cl.Append([]byte(`123`))
        cl.Append([]byte(`456`))
        cl.Append([]byte(`789`))

        _, err = cl.Read(10)
        if err != commitlog.ErrorRecordNotFound {
                t.Errorf("Expect nil error but got: %v", err)
        }

        cleanup(cl)
}

func BenchmarkWrite1KB(b *testing.B) {
        benchmarkWriteSize(b, 1024)
}
func BenchmarkWrite2KB(b *testing.B) {
        benchmarkWriteSize(b, 2048)
}
func BenchmarkWrite4KB(b *testing.B) {
        benchmarkWriteSize(b, 4096)
}

func benchmarkWriteSize(b *testing.B, size int) {
        cl, _ := commitlog.New("test.db", nil)
        data := make([]byte, size)

        for i := 0; i < b.N; i++ {
                cl.Append(data)
        }

        cleanup(cl)
}

func cleanup(cl *commitlog.CommitLog) {
	os.RemoveAll(cl.Path)
}
