package commitlog_test

import (
        "os"
        "testing"

        "github.com/HoMuChen/commitlog"
)

const (
        INDEX_DIR = "index.db"
)

func TestWrite(t *testing.T) {
        setupIndexDir(INDEX_DIR)

        index, err := commitlog.NewIndex(INDEX_DIR, 0, nil)

        if err != nil {
                t.Error(err)
        }

        index.Write(0, 0)
        index.Write(1, 1)
        index.Write(2, 2)
        index.Sync()
        index.Load()
        data := index.Data()

        if data[0] != 0 || data[1] != 1 || data[2] != 2 {
                t.Error("Incorrect in memory hash table data")
        }

        cleanupIndex()
}

func TestReopen(t *testing.T) {
        setupIndexDir(INDEX_DIR)

        index, err := commitlog.NewIndex(INDEX_DIR, 1, nil)
        if err != nil {
                t.Error(err)
        }

        index.Write(0, 0)
        index.Write(1, 1)
        index.Write(2, 2)
        index.Sync()
        index.Close()

        index, err = commitlog.NewIndex("index.db", 1, nil)
        if err != nil {
                t.Error(err)
        }

        index.Load()
        data := index.Data()

        if data[0] != 0 || data[1] != 1 || data[2] != 2 {
                t.Error("Incorrect in memory hash table data")
        }

        cleanupIndex()
}

func setupIndexDir(path string) {
        os.MkdirAll(path, 0755)
}

func cleanupIndex() {
	os.RemoveAll(INDEX_DIR)
}
