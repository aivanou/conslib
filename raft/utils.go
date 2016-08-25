package main

import "time"

func isLaterThan(oldTime, newTime time.Time) bool {
	cmp := compareTime(newTime, oldTime)
	return cmp >= 0
}

func compareTime(t1, t2 time.Time) int {
	return compareInt64(t1.UnixNano(), t2.UnixNano())
}

func compareInt64(v1, v2 int64) int {
	if v1 == v2 {
		return 0
	} else if v1 > v2 {
		return 1
	}else {
		return 2
	}
}