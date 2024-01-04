package wal

import (
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/rosedblabs/wal"
)

func BenchmarkWrite(b *testing.B) {
	b.Run("wal-sync", func(b *testing.B) {
		os.RemoveAll("tmp")
		opt := DefaultOptions
		wal, _ := Open("tmp", opt)

		for i := 0; i < b.N; i++ {
			data := fmt.Sprintf("%12d", i)
			wal.Write(uint64(i), []byte(data))
		}
		wal.Close()
	})

	b.Run("wal-nosync", func(b *testing.B) {
		os.RemoveAll("tmp")
		opt := DefaultOptions
		opt.NoSync = true
		wal, _ := Open("tmp", opt)

		for i := 0; i < b.N; i++ {
			data := fmt.Sprintf("%12d", i)
			wal.Write(uint64(i), []byte(data))
		}
		wal.Close()
	})

	b.Run("rosedb-sync", func(b *testing.B) {
		os.RemoveAll("tmp")
		opt := wal.DefaultOptions
		opt.DirPath = "tmp"
		opt.Sync = true
		wal, _ := wal.Open(opt)

		for i := 0; i < b.N; i++ {
			data := fmt.Sprintf("%12d", i)
			wal.Write([]byte(data))
		}

		wal.Close()
	})

	b.Run("rosedb/wal-nosync", func(b *testing.B) {
		os.RemoveAll("tmp")
		opt := wal.DefaultOptions
		opt.DirPath = "tmp"
		wal, _ := wal.Open(opt)

		for i := 0; i < b.N; i++ {
			data := fmt.Sprintf("%12d", i)
			wal.Write([]byte(data))
		}
		wal.Close()
	})
}

func BenchmarkRead(b *testing.B) {
	const NUM = 100 * 10000

	b.Run("wal", func(b *testing.B) {
		// init log data.
		os.RemoveAll("tmp")
		opt := DefaultOptions
		opt.NoSync = true
		w, _ := Open("tmp", opt)
		for i := 0; i < NUM; i++ {
			data := fmt.Sprintf("%12d", i)
			w.Write(uint64(i), []byte(data))
		}
		w.Close()

		// reopen.
		w, _ = Open("tmp", opt)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			w.Read(uint64(i % NUM))
		}
	})

	b.Run("rosedb/wal", func(b *testing.B) {
		// init log data.
		os.RemoveAll("tmp")
		opt := wal.DefaultOptions
		opt.DirPath = "tmp"
		opt.BytesPerSync = math.MaxUint32
		posSlice := make([]*wal.ChunkPosition, 0, NUM)
		w, _ := wal.Open(opt)

		for i := 0; i < NUM; i++ {
			data := fmt.Sprintf("%12d", i)
			pos, _ := w.Write([]byte(data))
			posSlice = append(posSlice, pos)
		}
		w.Close()

		// reopen.
		w, _ = wal.Open(opt)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			w.Read(posSlice[i%NUM])
		}
	})
}
