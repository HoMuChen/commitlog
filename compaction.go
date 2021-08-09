package commitlog

import (
        "time"
)

func (cl *CommitLog) Compact() {
        tm := time.Now().Add(-1 * cl.options.RetentionPolicy)

        lastSegment := 0
        for _, seg := range cl.segments {
                off, err := seg.timeindex.lastOffsetBeforeTm(tm)
                if err != nil {

                }
                if off == -2 {
                        lastSegment++
                } else {
                        break
                }
        }

        cl.mu.Lock()
        for i := 0; i < lastSegment; i++ {
                cl.segments[i].Remove()
        }
        cl.segments = cl.segments[lastSegment:len(cl.segments)]
        cl.mu.Unlock()
}
