package commitlog

import (
        "encoding/binary"
        "errors"
        "fmt"
        "math"
        "os"
        "path/filepath"
)

var (
        ErrorExceedMaxRecordSize = errors.New("Record is too big")
)

const (
        SegExt           = ".log"

        maxRecordSize    = math.MaxUint16
)

type segment struct {
        path            string
        options         *Options
        f               *os.File
        index           *Index
        //timeIndex     *TimeIndex //TODO: index for retention policy
        baseOffset      int        // first record offset, same as file name
        count           int        // relative offset in this segemnt
        position        int        // relative byte position in this segment file of next record
        isLoaded        bool
        isFull          bool
}

func NewSegment(dir string, offset int, options *Options) (*segment, error) {
        name := fmt.Sprintf("%020d", offset)
        path := filepath.Join(dir, name + SegExt)

        f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
        if err != nil {
                return nil, err
        }

        idx, err := NewIndex(dir, offset, options)
        if err != nil {
                return nil, err
        }

        seg := &segment{
                path:           path,
                f:              f,
                options:        options,
                baseOffset:     offset,
                index:          idx,
        }

        return seg, nil
}

func (seg *segment) Load() error {
        if err := seg.index.Load(); err != nil {
                return err
        }
        seg.count = seg.index.Count()

        fi, err := seg.f.Stat()
        if err != nil {
                return err
        }
        seg.position = int(fi.Size())

        seg.isLoaded = true

        // inconsistency between log file and index file
        if lastIndexPosition, ok := seg.index.Get(seg.count); !ok || lastIndexPosition != seg.position {
                if err := seg.Recover(); err != nil {
                        return err
                }
        }

        return nil
}

//TODO
func (seg *segment) Recover() error {
        return nil
}

func (seg *segment) CheckFull(data []byte) bool {
        if !seg.isLoaded {
                seg.Load()
        }

        if len(data) + seg.position > seg.options.MaxSegmentSize {
                return true
        }

        return false
}

func (seg *segment) Write(data []byte) error {
        if len(data) > maxRecordSize {
                return ErrorExceedMaxRecordSize
        }

        record := seg.encodeSegmentRecord(data)

        n, err := seg.f.Write(record)
        if err != nil {
                return err
        }

        seg.index.Write(seg.count, seg.position)

        seg.count += 1
        seg.position += n

        return nil
}

// Segemnt record
// + --------------- + ----- +
// | Value Size (2B) | Value |
// + --------------- + ----- +
func (seg *segment) encodeSegmentRecord(data []byte) []byte {
        buf := make([]byte, 2 + len(data))

        binary.LittleEndian.PutUint16(buf[:2], uint16(len(data)))
        copy(buf[2:], data)

        return buf
}

func (seg *segment) Read(offset int) ([]byte, error) {
        from, to, err := seg.getRecordPosition(offset)
        if err != nil {
                return nil, err
        }

        data := make([]byte, to - from)

        _, err = seg.f.ReadAt(data, int64(from))
        if err != nil {
                return nil, err
        }

        return data[2:], nil
}

func (seg *segment) getRecordPosition(offset int) (from, to int, err error) {
        position, ok := seg.index.Get(offset - seg.baseOffset)
        if !ok {
                return -1, -1, ErrorRecordNotFound
        }
        nextPosition, ok := seg.index.Get(offset - seg.baseOffset + 1)
        //last record
        if !ok {
                return position, seg.position, nil
        }

        return position, nextPosition, nil
}

func (seg *segment) NextOffset() int {
        return seg.baseOffset + seg.count
}

func (seg *segment) ClearCache() error {
        seg.index.ClearCache()

        return nil
}

func (seg *segment) Sync() (err error) {
        err = seg.f.Sync()
        err = seg.index.Sync()

        return
}

func (seg *segment) Close() (err error) {
        err = seg.f.Sync()
        err = seg.index.Sync()

        return
}
