package main

// checkedAnswer is one question after the exam service has sent both numbers.
type checkedAnswer struct {
	Selected int
	Correct  int
}

// grade counts equal questions. Every question is worth the same. There is no partial credit.
func grade(answers []checkedAnswer) (correct, total, percentage int) {
	total = len(answers)
	for _, answer := range answers {
		if answer.Selected == answer.Correct {
			correct++
		}
	}
	if total == 0 {
		return 0, 0, 0
	}
	percentage = correct * 100 / total
	return correct, total, percentage
}
