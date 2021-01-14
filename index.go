package commitlog

import (
        "encoding/binary"
        "fmt"
        "io/ioutil"
        "os"
        "path/filepath"
)

const (
        IndexExt = ".index"
)

type Index struct {
        Path            string
        f               *os.File
        baseOffset      int
        data            map[int]int
}

func NewIndex(dir string, offset int, options *Options) (*Index, error) {
        name := fmt.Sprintf("%020d", offset)
        path := filepath.Join(dir, name + IndexExt)

        f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
        if err != nil {
                return nil, err
        }

        idx := &Index{
                Path:           path,
                f:              f,
                baseOffset:     offset,
        }

        return idx, nil
}

func (idx *Index) Load() error {
        if idx.data == nil {
                idx.data = make(map[int]int)
        }

        idx.f.Seek(0, 0)
        data, err := ioutil.ReadAll(idx.f)
        if err != nil {
                return err
        }

        for len(data) > 0 {
                offset, o := binary.Uvarint(data)
                data = data[o:]

                position, p := binary.Uvarint(data)
                data = data[p:]

                idx.data[int(offset)] = int(position)
        }

        return nil
}

func (idx *Index) Count() int {
        if idx.data == nil {
                idx.Load()
        }

        return len(idx.data)
}

func (idx *Index) Write(offset int, position int) error {
        data := idx.encodeIndexRecord(offset, position)
        _, err := idx.f.Write(data)

        return err
}

func (idx *Index) encodeIndexRecord(offset int, position int) []byte {
        buf := make([]byte, binary.MaxVarintLen64)

        of := binary.PutUvarint(buf, uint64(offset))
        pos := binary.PutUvarint(buf[of:], uint64(position))

        return buf[:of+pos]
}

func (idx *Index) Data() map[int]int {
        return idx.data
}

func (idx *Index) Sync() error {
        return idx.f.Sync()
}

func (idx *Index) Close() error {
        return idx.f.Close()
}
