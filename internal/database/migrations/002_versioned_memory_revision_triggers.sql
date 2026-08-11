DROP TRIGGER IF EXISTS memories_revision_trigger ON memories;
DROP TRIGGER IF EXISTS memories_insert_revision_trigger ON memories;
DROP TRIGGER IF EXISTS memories_update_revision_trigger ON memories;

CREATE TRIGGER memories_insert_revision_trigger
    AFTER INSERT ON memories
    FOR EACH ROW EXECUTE FUNCTION record_memory_revision();

CREATE TRIGGER memories_update_revision_trigger
    AFTER UPDATE ON memories
    FOR EACH ROW
    WHEN (OLD.version IS DISTINCT FROM NEW.version)
    EXECUTE FUNCTION record_memory_revision();
