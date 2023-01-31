package test

import (
	"testing"
	"time"

	"github.com/xgzlucario/rotom/app"
)

var defaultSign = getSignIn()

func getSignIn() *app.SignIn {
	s := app.NewSignIn()
	now := time.Now()
	for i := 0; i < billion; i++ {
		s.AddRecord(1, now)
		now = now.Add(time.Hour * 24)
	}
	return s
}

func BenchmarkSignIn1(b *testing.B) {
	s := app.NewSignIn()
	now := time.Now()
	for i := 0; i < b.N; i++ {
		s.AddRecord(uint32(i), now)
	}
}

func BenchmarkSignIn2(b *testing.B) {
	s := app.NewSignIn()
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
