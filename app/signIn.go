package app

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/structx"
)

// toDateTime: Return time by dateID
func (s *SignIn) toDateTime(dateID uint32) time.Time {
	return ZeroTime.Add(time.Duration(dateID) * DateDuration)
}

// toDateID: Return days to ZeroTime
func (s *SignIn) toDateID(date time.Time) uint32 {
	return uint32(date.Sub(ZeroTime) / DateDuration)
}

var (
	// ZeroTime: Make sure the sign date is greater than ZeroTime
	ZeroTime, _ = time.Parse("2006-01-02", "2023-01-01")

	// Sign date duration
	DateDuration = time.Hour * 24
)

// SignIn: Threadsafe Sign-In Data Structure
type SignIn struct {
	mu      sync.RWMutex
	dateMap structx.Map[uint32, *structx.BitMap]
	userMap structx.Map[uint32, *structx.BitMap]
}

// NewSignIn
func NewSignIn() *SignIn {
	return &SignIn{
		dateMap: structx.NewMap[uint32, *structx.BitMap](),
		userMap: structx.NewMap[uint32, *structx.BitMap](),
	}
}

// AddRecord: add a sign-in record
func (s *SignIn) AddRecord(userID uint32, date time.Time) error {
	dateID := s.toDateID(date)
	s.mu.Lock()
	defer s.mu.Unlock()

	fmt.Println(userID, dateID)

	// userRecord
	bm, ok := s.userMap.Get(userID)
	if !ok {
		bm = structx.NewBitMap()
		s.userMap.Set(userID, bm)
	}
	// check if signed in
	if ok = bm.Add(dateID); !ok {
		return errors.New("sign-in record already exist")
	}

	// dateRecord
	bm, ok = s.dateMap.Get(dateID)
	if !ok {
		bm = structx.NewBitMap()
		s.dateMap.Set(dateID, bm)
	}
	// check if signed in
	if ok = bm.Add(userID); !ok {
		return errors.New("sign-in record already exist")
	}

	return nil
}

// UserCount: Get the number of days users have signed in
// 用户签到总天数
func (s *SignIn) UserCount(userId uint32) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	bm, ok := s.userMap.Get(userId)
	if !ok {
		return 0, errors.New("userId not exist")
	}

	return bm.Len(), nil
}

// UserSignDates: Get user sign-in dates order by DESC, you can set limit of return numbers.
// 用户签到日期列表
func (s *SignIn) UserSignDates(userId uint32, limits ...int) []time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()

	bm, ok := s.userMap.Get(userId)
	if !ok {
		return nil
	}

	// limit
	var limit = bm.Len()
	if len(limits) > 0 {
		limit = limits[0]
	}

	// parse timeSlice
	times := make([]time.Time, 0, limit)
	var count int

	bm.RevRange(func(id uint32) bool {
		times = append(times, s.toDateTime(id))
		count++
		return count > limit
	})

	return times
}

// UserRecentDate: Get the user's most recent sign-in date
// 用户最近签到日期
func (s *SignIn) UserRecentDate(userId uint32) time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()

	bm, ok := s.userMap.Get(userId)
	if !ok {
		return time.Time{}
	}

	return s.toDateTime(uint32(bm.Max()))
}

// DateCount: Get the total number of sign-in for the day
// 当日签到总量统计
func (s *SignIn) DateCount(date time.Time) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	id := s.toDateID(date)
	bm, ok := s.dateMap.Get(id)
	if !ok {
		return -1, base.ErrKeyNotFound(date)
	}

	return bm.Len(), nil
}

// marshal type
type signInJSON struct {
	D structx.Map[uint32, *structx.BitMap]
	U structx.Map[uint32, *structx.BitMap]
}

func (s *SignIn) MarshalJSON() ([]byte, error) {
	return base.MarshalJSON(signInJSON{s.dateMap, s.userMap})
}

func (s *SignIn) UnmarshalJSON(src []byte) error {
	var tmp signInJSON
	if err := base.UnmarshalJSON(src, &tmp); err != nil {
		return err
	}

	s.dateMap = tmp.D
	s.userMap = tmp.U
	return nil
}
