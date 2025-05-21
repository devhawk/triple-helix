import postgres, { type Sql } from "postgres";

// helper functions to create/drop the database
async function checkDB(sql: Sql, name: string) {
    const results =
        await sql/*sql*/ `SELECT 1 FROM pg_database WHERE datname = ${name}`.values();
    return results.length > 0;
}

async function ensureDB(sql: Sql, name: string) {
    const exists = await checkDB(sql, name);
    if (!exists) {
        await sql.unsafe(/*sql*/ `CREATE DATABASE ${name}`).simple();
    }
}

async function dropDB(sql: Sql, name: string) {
    const exists = await checkDB(sql, name);
    if (exists) {
        await sql.unsafe(/*sql*/ `DROP DATABASE ${name}`).simple();
    }
}

export async function setupDB(config: postgres.Options<{}>) {
    if (config.database === undefined) {
        throw new Error("Database name is required");
    }
    
    {
        const pg = postgres({ ...config, database: "postgres", onnotice: () => {} });
        try {
            await ensureDB(pg, config.database);
        } finally {
            await pg.end();
        }
    }

    {
        const pg = postgres({ ...config, onnotice: () => {} });
        try {
            await pg/*sql*/`
CREATE EXTENSION IF NOT EXISTS plv8;
CREATE SCHEMA IF NOT EXISTS dbos;

CREATE TABLE IF NOT EXISTS dbos.transaction_outputs (
    workflow_id TEXT NOT NULL,
    function_num INT NOT NULL,
    output TEXT,
    created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint,
    PRIMARY KEY (workflow_id, function_num));

CREATE OR REPLACE FUNCTION dbos.check_execution(_workflow_id TEXT, _function_num INT)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    _output JSONB;
BEGIN
    SELECT output INTO _output 
    FROM dbos.transaction_outputs
    WHERE workflow_id = _workflow_id AND function_num = _function_num;

    IF jsonb_typeof(_output) <> 'null' THEN
        RETURN jsonb_build_object('output', _output);
    END IF;

    RETURN NULL;
END;
$$;

-- this function is *NOT what we expect to see in the final version
CREATE OR REPLACE FUNCTION "triple_helix_demo_f"(
    _workflow_id TEXT,
    _context JSONB,
    _params JSONB
) RETURNS JSONB AS $$ 

    function sampleTxStep(step) {
        try {
            const result = plv8.execute("SELECT $1::int AS step", [step]);
            return result[0].step;
        }
        finally {
            plv8.elog(INFO, "Completed triple_helix_demo_f::sampleTxStep " + JSON.stringify(step));
        }
    }

    function $serialize_error(e) {
        return { error: { name: e.name, message: e.message, stack: e.stack } };
    }

    function $run(func, args) {
        try {
            const output = func(...args);
            return { output };
        } catch (e) {
            return $serialize_error(e);
        }
    }

    return $run(sampleTxStep, _params);
    
$$ LANGUAGE plv8;

-- this stored proc is approximately what we expect to see in the final version
CREATE OR REPLACE PROCEDURE "triple_helix_demo_p"(
    _workflow_id TEXT, 
    _function_num INT, 
    _params JSONB,
    _context JSONB,
    OUT return_value JSONB
)
LANGUAGE plpgsql
as $$
DECLARE
    _output JSONB;
    _error JSONB;
    _snapshot TEXT;
    _txn_id TEXT;
BEGIN

    SELECT dbos.check_execution(_workflow_id, _function_num) INTO return_value;
    IF return_value IS NOT NULL THEN
        RETURN;
    END IF;

    SELECT "triple_helix_demo_f"(_workflow_id, _context, _params) INTO return_value;
    SELECT pg_current_snapshot(), pg_current_xact_id_if_assigned() INTO _snapshot, _txn_id;
    SELECT return_value::jsonb->'output', return_value::jsonb->'error' INTO _output, _error;

    IF _error IS NOT NULL OR jsonb_typeof(_error) <> 'null' THEN
        ROLLBACK;
    ELSE 
        INSERT INTO dbos.transaction_outputs (workflow_id, function_num, output)
        VALUES (_workflow_id, _function_num, _output);
        
        SELECT return_value::jsonb || jsonb_build_object('txn_snapshot', _snapshot, 'txn_id', _txn_id) INTO return_value;
    END IF;

END; $$;
                `.simple();

        } finally {
            await pg.end();
        }
    }
}
