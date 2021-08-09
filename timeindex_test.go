package commitlog

import (
        "testing"
        "time"
)

const (
        TIMEINDEX_DIR = "timeindex.db"
)

func TestTimeIndexWrite(t *testing.T) {
        setupDir(TIMEINDEX_DIR)
        defer cleanupDir(TIMEINDEX_DIR)

        timeindex, err := NewTimeIndex(TIMEINDEX_DIR, 0, nil)

        if err != nil {
                t.Error(err)
        }

        timeindex.Write(time.Now().Add(1 * time.Minute), 0)
        timeindex.Write(time.Now().Add(2 * time.Minute), 1)
        timeindex.Write(time.Now().Add(3 * time.Minute), 2)
        timeindex.Sync()

        timeindex.load()
        offset, _ := timeindex.lastOffsetBeforeTm(time.Now().Add(4 * time.Minute))

        if offset != -2 {
                t.Errorf("Expect -2 but got: %v", offset)
        }

        offset, _ = timeindex.lastOffsetBeforeTm(time.Now())
        if offset != -1 {
                t.Errorf("Expect -1 but got: %v", offset)
        }

        offset, _ = timeindex.lastOffsetBeforeTm(time.Now().Add(90 * time.Second))
        if offset != 0 {
                t.Errorf("Expect 0 but got: %v", offset)
        }
}
