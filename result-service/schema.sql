-- One row per exam, copied from the exam.created event.
CREATE TABLE IF NOT EXISTS exam_refs (
    exam_id BIGINT PRIMARY KEY,
    title TEXT NOT NULL,
    teacher_id BIGINT NOT NULL
);

-- One score per attempt. The exam service sends the chosen and correct options.
CREATE TABLE IF NOT EXISTS results (
    attempt_id BIGINT PRIMARY KEY,
    exam_id BIGINT NOT NULL,
    student_id BIGINT NOT NULL,
    correct_count INT NOT NULL,
    total_questions INT NOT NULL,
    percentage INT NOT NULL
);

-- One audit row per submit. This event has no answers and no score.
CREATE TABLE IF NOT EXISTS submission_log (
    attempt_id BIGINT PRIMARY KEY,
    exam_id BIGINT NOT NULL,
    student_id BIGINT NOT NULL,
    submitted_at TIMESTAMPTZ NOT NULL
);
