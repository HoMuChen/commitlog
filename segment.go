package commitlog

import (
        "fmt"
        "io/ioutil"
        "os"
        "path/filepath"
)

const (
        SegExt = ".log"
)

type segment struct {
        path            string
        f               *os.File
        index           *Index
        options         *Options
        baseOffset      int
        count           int
        position        int
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

        data, err := ioutil.ReadAll(seg.f)
        if err != nil {
                return err
        }

        seg.position = len(data)
        seg.isLoaded = true
        seg.count = seg.index.Count()

        return nil
}

func (seg *segment) CheckFull(data []byte) bool {
        if seg.isFull {
                return true
        }

        if !seg.isLoaded {
                seg.Load()
        }

        if len(data) + seg.position > seg.options.MaxSegmentSize {
                seg.isFull = true
                return true
        }

        return false
}

func (seg *segment) Write(data []byte) error {
        n, err := seg.f.Write(data)
        if err != nil {
                return err
        }

        seg.index.Write(seg.count, seg.position)

        seg.position += n
        seg.count += 1

        return nil
}

func (seg *segment) NextOffset() int {
        return seg.baseOffset + seg.count
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
