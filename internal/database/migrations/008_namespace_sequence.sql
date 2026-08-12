CREATE SEQUENCE namespaces_sequence_number_seq
    AS bigint
    MINVALUE 0
    START WITH 0;

ALTER TABLE namespaces
    ADD COLUMN sequence_number bigint;

WITH ordered_namespaces AS (
    SELECT
        code,
        row_number() OVER (ORDER BY created_at, code) - 1 AS sequence_number
    FROM namespaces
)
UPDATE namespaces AS namespace
SET sequence_number = ordered.sequence_number
FROM ordered_namespaces AS ordered
WHERE ordered.code = namespace.code;

SELECT setval(
    'namespaces_sequence_number_seq',
    coalesce((SELECT max(sequence_number) + 1 FROM namespaces), 0),
    false
);

ALTER SEQUENCE namespaces_sequence_number_seq
    OWNED BY namespaces.sequence_number;

ALTER TABLE namespaces
    ALTER COLUMN sequence_number SET DEFAULT nextval('namespaces_sequence_number_seq'),
    ALTER COLUMN sequence_number SET NOT NULL,
    ADD CONSTRAINT namespaces_sequence_number_key UNIQUE (sequence_number);
