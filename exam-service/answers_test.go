package main

import "testing"

func TestValidateAnswers(t *testing.T) {
	ids := []int64{10, 11}
	good := []answerInput{{QuestionID: 10, SelectedOption: 1}, {QuestionID: 11, SelectedOption: 4}}
	if err := validateAnswers(ids, good); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		answers []answerInput
		want    string
	}{
		{name: "missing one", answers: []answerInput{{QuestionID: 10, SelectedOption: 1}}, want: "answer every question once"},
		{name: "duplicate", answers: []answerInput{{QuestionID: 10, SelectedOption: 1}, {QuestionID: 10, SelectedOption: 2}}, want: "answer every question once"},
		{name: "wrong question", answers: []answerInput{{QuestionID: 10, SelectedOption: 1}, {QuestionID: 99, SelectedOption: 2}}, want: "question is not part of this exam"},
		{name: "bad option", answers: []answerInput{{QuestionID: 10, SelectedOption: 5}, {QuestionID: 11, SelectedOption: 1}}, want: "selected_option must be 1, 2, 3, or 4"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateAnswers(ids, test.answers)
			if err == nil || err.Error() != test.want {
				t.Fatalf("got %v, want %q", err, test.want)
			}
		})
	}
}
