-- exams: one test created by a teacher.
CREATE TABLE IF NOT EXISTS exams (
    id BIGSERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    teacher_id BIGINT NOT NULL,
    duration_minutes INT NOT NULL CHECK (duration_minutes > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- questions: correct_option is 1, 2, 3, or 4. Students never receive this column.
CREATE TABLE IF NOT EXISTS questions (
    id BIGSERIAL PRIMARY KEY,
    exam_id BIGINT NOT NULL REFERENCES exams (id),
    body TEXT NOT NULL,
    correct_option SMALLINT NOT NULL CHECK (correct_option BETWEEN 1 AND 4)
);

-- options: exactly four rows per question, numbered 1 to 4.
CREATE TABLE IF NOT EXISTS options (
    id BIGSERIAL PRIMARY KEY,
    question_id BIGINT NOT NULL REFERENCES questions (id),
    option_number SMALLINT NOT NULL CHECK (option_number BETWEEN 1 AND 4),
    body TEXT NOT NULL,
    UNIQUE (question_id, option_number)
);

-- attempts: one student can take one exam only once.
CREATE TABLE IF NOT EXISTS attempts (
    id BIGSERIAL PRIMARY KEY,
    exam_id BIGINT NOT NULL REFERENCES exams (id),
    student_id BIGINT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('started', 'submitted', 'graded')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    submitted_at TIMESTAMPTZ,
    UNIQUE (exam_id, student_id)
);

-- attempt_answers: the option the student picked for one question.
CREATE TABLE IF NOT EXISTS attempt_answers (
    id BIGSERIAL PRIMARY KEY,
    attempt_id BIGINT NOT NULL REFERENCES attempts (id),
    question_id BIGINT NOT NULL REFERENCES questions (id),
    selected_option SMALLINT NOT NULL CHECK (selected_option BETWEEN 1 AND 4),
    UNIQUE (attempt_id, question_id)
);
