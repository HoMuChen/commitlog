package commitlog

import (
        "bufio"
        "encoding/binary"
        "fmt"
        "io/ioutil"
        "os"
        "path/filepath"
        "time"
)

const (
        TimeIndexExt = ".timeindex"
)

type timeIndex struct {
        path            string
        f               *os.File
        writer          *bufio.Writer
        baseOffset      int
        createdAts      []uint32
        offsets         []uint64
}

func NewTimeIndex(dir string, offset int, options *Options) (*timeIndex, error) {
        name := fmt.Sprintf("%020d", offset)
        path := filepath.Join(dir, name + TimeIndexExt)

        f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
        if err != nil {
                return nil, err
        }

        idx := &timeIndex{
                path:           path,
                f:              f,
                writer:         bufio.NewWriter(f),
                baseOffset:     offset,
        }

        return idx, nil
}

func (idx *timeIndex) Write(tm time.Time, offset int) error {
        data := idx.encodeTimeIndexRecord(tm, offset)
        _, err := idx.writer.Write(data)

        return err
}

// Timeindex record
// + ----------------------- + ---------- +
// | Timestamp(seconds) (4B) | offset(8B) |
// + ----------------------- + ---------- +
func (idx *timeIndex) encodeTimeIndexRecord(tm time.Time, offset int) []byte {
        buf := make([]byte, 12)

        binary.LittleEndian.PutUint32(buf[:4], uint32(tm.Unix()))
        binary.LittleEndian.PutUint64(buf[4:], uint64(offset))

        return buf
}

func (idx *timeIndex) load() error {
        idx.createdAts = make([]uint32, 0)
        idx.offsets = make([]uint64, 0)
        idx.f.Seek(0, 0)

        data, err := ioutil.ReadAll(idx.f)
        if err != nil {
                return err
        }

        for len(data) > 0 {
                createdAt := binary.LittleEndian.Uint32(data[:4])
                data = data[4:]

                offset := binary.LittleEndian.Uint64(data[:8])
                data = data[8:]

                idx.createdAts = append(idx.createdAts, createdAt)
                idx.offsets = append(idx.offsets, offset)
        }

        return nil
}

func (idx *timeIndex) clearCache() error {
        idx.createdAts = make([]uint32, 0)
        idx.offsets = make([]uint64, 0)

        return nil
}

// -1 -> none
// -2 -> all
func (idx *timeIndex) lastOffsetBeforeTm(tm time.Time) (int, error) {
        timestamp := uint32(tm.Unix())

        for i, createdAt := range idx.createdAts {
                if createdAt > timestamp && i == 0 {
                        return -1, nil
                }

                if createdAt > timestamp {
                        return i-1, nil
                }
        }

        return -2, nil
}

func (idx *timeIndex) Sync() error {
        return idx.writer.Flush()
}

func (idx *timeIndex) Close() error {
        return idx.f.Close()
}

func (idx *timeIndex) Remove() error {
        if err := idx.Close(); err != nil {
                return err
        }

        return os.Remove(idx.path)
}
