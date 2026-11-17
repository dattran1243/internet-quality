-- TEST-ONLY: manual application to isolated viq_s14_test_<12 hex> database only.
-- Does NOT migrate VIQ production registry. No auto invocation from scripts.
BEGIN;
DO $guard$
BEGIN
 IF current_database() !~ '^viq_s14_test_[a-f0-9]{12}$' THEN
  RAISE EXCEPTION 'BLOCKED_NOT_ISOLATED_VIQ_STAGE14_DATABASE';
 END IF;
END $guard$;
CREATE SCHEMA IF NOT EXISTS viq_s14test;
CREATE TABLE IF NOT EXISTS viq_s14test.generations (
 namespace text NOT NULL CHECK(namespace ~ '^s14test_[a-f0-9]{12}$'),
 hour_utc text NOT NULL CHECK(hour_utc ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:00:00Z$'),
 generation text NOT NULL CHECK(generation ~ '^[a-f0-9]{32}$'),
 digest text NOT NULL CHECK(digest ~ '^[a-f0-9]{64}$'),
 test_only boolean NOT NULL DEFAULT true CHECK (test_only),
 PRIMARY KEY(namespace,hour_utc,generation)
);
CREATE TABLE IF NOT EXISTS viq_s14test.heads (
 namespace text NOT NULL CHECK(namespace ~ '^s14test_[a-f0-9]{12}$'),
 hour_utc text NOT NULL,
 epoch bigint NOT NULL CHECK(epoch > 0),
 generation text NOT NULL,
 digest text NOT NULL,
 PRIMARY KEY(namespace,hour_utc),
 FOREIGN KEY(namespace,hour_utc,generation)
  REFERENCES viq_s14test.generations(namespace,hour_utc,generation)
);
CREATE OR REPLACE FUNCTION viq_s14test.reject_generation_mutation()
RETURNS trigger LANGUAGE plpgsql AS $BODY$
BEGIN RAISE EXCEPTION 'IMMUTABLE_SANDBOX_GENERATION'; END
$BODY$;
DROP TRIGGER IF EXISTS trg_immutable_s14_generations ON viq_s14test.generations;
CREATE TRIGGER trg_immutable_s14_generations BEFORE UPDATE OR DELETE
 ON viq_s14test.generations FOR EACH ROW
 EXECUTE FUNCTION viq_s14test.reject_generation_mutation();
COMMIT;
