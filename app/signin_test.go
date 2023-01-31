package app

import (
	"testing"
	"time"
)

const (
	billion      = 100000000      // 10^9
	_100million  = million * 100  // 10^8
	_10million   = million * 10   // 10^7
	million      = 1000000        // 10^6
	_100thousand = thousand * 100 // 10^5
	_10thousand  = thousand * 10  // 10^4
	thousand     = 1000           // 10^3
)

var defaultSign = getSignIn()

func getSignIn() *SignIn {
	s := NewSignIn()
	now := time.Now()
	for i := 0; i < billion; i++ {
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
