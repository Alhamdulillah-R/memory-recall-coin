ALTER TABLE board_messages ADD COLUMN IF NOT EXISTS created_by_session text NOT NULL DEFAULT '';
