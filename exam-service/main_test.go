package main

import "testing"

func validExam() examRequest {
	return examRequest{
		Title:           "Go basics",
		TeacherID:       1,
		DurationMinutes: 20,
		Questions: []questionRequest{
			{
				Body:          "What is 2 + 2?",
				CorrectOption: 4,
				Options:       []string{"3", "5", "6", "4"},
			},
		},
	}
}

func TestValidateExam(t *testing.T) {
	tests := []struct {
		name    string
		change  func(*examRequest)
		wantErr string
	}{
		{name: "good exam"},
		{
			name:    "blank title",
			change:  func(exam *examRequest) { exam.Title = "   " },
			wantErr: "title is required",
		},
		{
			name:    "teacher id is zero",
			change:  func(exam *examRequest) { exam.TeacherID = 0 },
			wantErr: "teacher_id must be a positive number",
		},
		{
			name:    "duration is zero",
			change:  func(exam *examRequest) { exam.DurationMinutes = 0 },
			wantErr: "duration_minutes must be greater than 0",
		},
		{
			name:    "no questions",
			change:  func(exam *examRequest) { exam.Questions = nil },
			wantErr: "exam needs at least one question",
		},
		{
			name: "blank question",
			change: func(exam *examRequest) {
				exam.Questions[0].Body = " "
			},
			wantErr: "question text is required",
		},
		{
			name: "correct option is 0",
			change: func(exam *examRequest) {
				exam.Questions[0].CorrectOption = 0
			},
			wantErr: "correct_option must be 1, 2, 3, or 4",
		},
		{
			name: "correct option is 5",
			change: func(exam *examRequest) {
				exam.Questions[0].CorrectOption = 5
			},
			wantErr: "correct_option must be 1, 2, 3, or 4",
		},
		{
			name: "three options",
			change: func(exam *examRequest) {
				exam.Questions[0].Options = []string{"a", "b", "c"}
			},
			wantErr: "each question needs exactly 4 options",
		},
		{
			name: "blank option",
			change: func(exam *examRequest) {
				exam.Questions[0].Options[2] = "  "
			},
			wantErr: "option text is required",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exam := validExam()
			if test.change != nil {
				test.change(&exam)
			}

			err := validateExam(exam)
			if test.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || err.Error() != test.wantErr {
				t.Fatalf("got %v, want %q", err, test.wantErr)
			}
		})
	}
}
