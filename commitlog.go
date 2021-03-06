package commitlog

import (
        "errors"
        "io/ioutil"
        "os"
        "strconv"
        "strings"
        "sync"
        "time"
)

var (
        ErrorRecordNotFound = errors.New("Record Not Found")
        ErrorSegmentNotFound = errors.New("Segment Not Found")
)

const (
        DefaultMaxSegmentSize           = 20 * 1024 * 1024
        DefaultCompactionInterval       = 12 * time.Hour
        DefaultRetentionPolicy          = 7 * 24 * time.Hour
)

type CommitLog struct {
        Path            string
        options         *Options
        segments        []*segment
        curSegment      *segment
        mu              sync.Mutex
        workerDone      chan bool
}

type Options struct {
        MaxSegmentSize          int
        CompactionInterval      time.Duration
        RetentionPolicy         time.Duration
}

func NewDefaultOptions() *Options {
        return &Options{
                MaxSegmentSize:         DefaultMaxSegmentSize,
                CompactionInterval:     DefaultCompactionInterval,
                RetentionPolicy:        DefaultRetentionPolicy,
        }
}

func New(path string, options *Options) (*CommitLog, error) {
        if options == nil {
                options = NewDefaultOptions()
        }

        cl := &CommitLog{
                Path:           path,
                options:        options,
                workerDone:     make(chan bool),
        }

        if err := cl.init(); err != nil {
                return nil, err
        }

        if err := cl.open(); err != nil {
                return nil, err
        }

        if err := cl.startWorker(); err != nil {
                return nil, err
        }

        return cl, nil
}

func (cl *CommitLog) init() error {
        if err := os.MkdirAll(cl.Path, 0755); err != nil {
                return err
        }

        return nil
}

func (cl *CommitLog) open() error {
        files, err := ioutil.ReadDir(cl.Path)

        if err != nil {
                return err
        }

        for _, file := range files {
                fileName := file.Name()
                if !strings.HasSuffix(fileName, SegExt) {
                        continue
                }

                offset, err := strconv.Atoi(strings.TrimSuffix(file.Name(), SegExt))
                if err != nil {
                        return err
                }

                seg, err := NewSegment(cl.Path, offset, cl.options)
                if err != nil {
                        return err
                }

                cl.segments = append(cl.segments, seg)
                cl.curSegment = seg
        }

        if len(files) == 0 {
                if err := cl.createNewSegment(0); err != nil {
                        return err
                }
        }

        if err := cl.curSegment.Load(); err != nil {
                return err
        }

        return nil
}

func (cl *CommitLog) startWorker() error {
        go func() {
                ticker := time.NewTicker(cl.options.CompactionInterval)

                for {
                        select {
                        case <- cl.workerDone:
                                break
                        case <- ticker.C:
                                cl.Compact()
                        }
                }
        }()

        return nil
}

func (cl *CommitLog) stopWorker() {
        close(cl.workerDone)
}

func (cl *CommitLog) createNewSegment(offset int) error {
        seg, err := NewSegment(cl.Path, offset, cl.options)
        if err != nil {
                return err
        }

        if cl.curSegment != nil {
                cl.curSegment.clearCache()
        }

        cl.segments = append(cl.segments, seg)
        cl.curSegment = seg

        if err := cl.curSegment.Load(); err != nil {
                return err
        }

        return nil
}

func (cl *CommitLog) Append(data []byte) (int, error) {
        cl.mu.Lock()
        defer cl.mu.Unlock()

        offset := cl.curSegment.NextOffset()

        if cl.curSegment.CheckFull(data) {
                cl.createNewSegment(offset)
        }

        if err := cl.curSegment.Write(data); err != nil {
                return 0, err
        }

        return offset, nil
}

func (cl *CommitLog) Read(offset int) ([]byte, error) {
        i, err := cl.findSegmentIndex(offset)
        if err != nil {
                return nil, err
        }

        return cl.segments[i].Read(offset)
}

func (cl *CommitLog) findSegmentIndex(offset int) (int, error) {
        for i := 0; i < len(cl.segments)-1; i++ {
                if offset < cl.segments[i].baseOffset {
                        return -1, ErrorSegmentNotFound
                }
                if offset >= cl.segments[i].baseOffset && offset < cl.segments[i+1].baseOffset {
                        return i, nil
                }
        }

        return len(cl.segments) - 1, nil
}

func (cl *CommitLog) Offset() int {
        return cl.curSegment.NextOffset() - 1
}

func (cl *CommitLog) Close() error {
        if err := cl.curSegment.Sync(); err != nil {
                return err
        }

        for _, seg := range cl.segments {
                if err := seg.Close(); err != nil {
                        return err
                }
        }

        cl.stopWorker()

        return nil
}
