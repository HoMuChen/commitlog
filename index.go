package commitlog

import (
        "bufio"
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
        writer          *bufio.Writer
        baseOffset      int
        data            map[int]int //in-momery index data
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
                writer:         bufio.NewWriter(f),
                baseOffset:     offset,
                data:           make(map[int]int),
        }

        return idx, nil
}

//Populate in-memory data, loading from disk
func (idx *Index) Load() error {
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
        return len(idx.data)
}

func (idx *Index) Write(offset int, position int) error {
        data := idx.encodeIndexRecord(offset, position)
        _, err := idx.writer.Write(data)

        idx.data[offset] = position

        return err
}

func (idx *Index) encodeIndexRecord(offset int, position int) []byte {
        buf := make([]byte, binary.MaxVarintLen64)

        of := binary.PutUvarint(buf, uint64(offset))
        pos := binary.PutUvarint(buf[of:], uint64(position))

        return buf[:of+pos]
}

func (idx *Index) Get(offset int) (int, bool) {
        pos, found := idx.data[offset]

        return pos, found
}

func (idx *Index) ClearCache() error {
        idx.data = make(map[int]int)

        return nil
}

func (idx *Index) Sync() error {
        return idx.writer.Flush()
}

func (idx *Index) Close() error {
        return idx.f.Close()
}
