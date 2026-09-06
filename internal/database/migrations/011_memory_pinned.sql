-- pinned 是「標記重要」的一等欄位；memory_pin 以前只清 TTL，agent 只能用標題前綴／tag／metadata 各自發明慣例
ALTER TABLE memories ADD COLUMN IF NOT EXISTS pinned boolean NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS memories_pinned_idx ON memories(namespace, updated_at DESC) WHERE pinned;
