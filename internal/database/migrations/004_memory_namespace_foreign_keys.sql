ALTER TABLE memories
    ADD CONSTRAINT memories_namespace_id_key UNIQUE (namespace, id);

ALTER TABLE memories
    DROP CONSTRAINT IF EXISTS memories_supersedes_id_fkey;

ALTER TABLE memories
    ADD CONSTRAINT memories_supersedes_namespace_fkey
    FOREIGN KEY (namespace, supersedes_id)
    REFERENCES memories(namespace, id);

ALTER TABLE memory_relations
    DROP CONSTRAINT IF EXISTS memory_relations_subject_memory_id_fkey;

ALTER TABLE memory_relations
    DROP CONSTRAINT IF EXISTS memory_relations_object_memory_id_fkey;

ALTER TABLE memory_relations
    ADD CONSTRAINT memory_relations_subject_namespace_fkey
    FOREIGN KEY (namespace, subject_memory_id)
    REFERENCES memories(namespace, id);

ALTER TABLE memory_relations
    ADD CONSTRAINT memory_relations_object_namespace_fkey
    FOREIGN KEY (namespace, object_memory_id)
    REFERENCES memories(namespace, id);
