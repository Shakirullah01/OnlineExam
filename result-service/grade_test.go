package main

import "testing"

func TestGrade(t *testing.T) {
	tests := []struct {
		name       string
		answers    []checkedAnswer
		correct    int
		total      int
		percentage int
	}{
		{name: "empty"},
		{
			name:       "all correct",
			answers:    []checkedAnswer{{1, 1}, {2, 2}, {4, 4}},
			correct:    3,
			total:      3,
			percentage: 100,
		},
		{
			name:       "none correct",
			answers:    []checkedAnswer{{1, 2}, {1, 3}},
			correct:    0,
			total:      2,
			percentage: 0,
		},
		{
			name:       "four of five",
			answers:    []checkedAnswer{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {1, 2}},
			correct:    4,
			total:      5,
			percentage: 80,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			correct, total, percentage := grade(test.answers)
			if correct != test.correct || total != test.total || percentage != test.percentage {
				t.Fatalf("got %d/%d = %d%%", correct, total, percentage)
			}
		})
	}
}
