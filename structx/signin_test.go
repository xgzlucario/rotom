package structx

import (
	"testing"
	"time"
)

var defaultSign = getSignIn()

func getSignIn() *SignIn {
	s := NewSignIn()
	now := time.Now()
	for i := 0; i < 1000000; i++ {
		s.AddRecord(1, now)
		now = now.Add(time.Hour * 24)
	}
	return s
}

func BenchmarkSignIn1(b *testing.B) {
	s := NewSignIn()
	now := time.Now()
	for i := 0; i < b.N; i++ {
		s.AddRecord(uint32(i), now)
	}
}

func BenchmarkSignIn2(b *testing.B) {
	s := NewSignIn()
	now := time.Now()
	for i := 0; i < b.N; i++ {
		s.AddRecord(1, now)
		now = now.Add(time.Hour * 24)
	}
}

func BenchmarkDateCount(b *testing.B) {
	for i := 0; i < b.N; i++ {
		defaultSign.DateCount(time.Now())
	}
}

func BenchmarkUserCount(b *testing.B) {
	for i := 0; i < b.N; i++ {
		defaultSign.UserCount(1)
	}
}

func BenchmarkUserRecentDate(b *testing.B) {
	for i := 0; i < b.N; i++ {
		defaultSign.UserRecentDate(1)
	}
}

func BenchmarkUserDates(b *testing.B) {
	for i := 0; i < b.N; i++ {
		defaultSign.UserSignDates(1, 100)
	}
}
