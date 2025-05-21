CREATE EXTENSION IF NOT EXISTS plv8;
CREATE SCHEMA IF NOT EXISTS dbos;

CREATE OR REPLACE FUNCTION "triple_helix_demo_f"(
    _workflow_id TEXT,
    _params JSONB
) RETURNS JSONB AS $$ 

    function $query(queryText, values) {
        const result = plv8.execute(queryText, values ?? []);
        plv8.elog(DEBUG1, "DBOSQuery.query", "result", JSON.stringify(result, (k, v) => typeof v === "bigint" ? v.toString() + "n" : v));
        if (typeof result === 'number') {
            return { rowCount: result };
        } else if (Array.isArray(result)) {
            return { rowCount: result.length, rows: result };
        } else {
            throw new Error(`unexpected result from plv8.execute ${typeof result}`);
        }
    };

    function $serialize_error(e) {
        // -- TODO: investigate using serialize-error package
        return { error: { name: e.name, message: e.message, stack: e.stack } };
    }

    function sampleTxStep(step) {
        try {
            const result = $query("SELECT $1::int AS step", [step]);
            return result.rows[0].step;
        } finally {
            plv8.elog(INFO, `Completed sampleTxStep ${step}!`);
        }
    }

    try {
        const output = sampleTxStep(..._params);
        return { output };
    } catch (e) {
        return $serialize_error(e);
    }
    
$$ LANGUAGE plv8;

CREATE OR REPLACE PROCEDURE "triple_helix_demo_p"(
    _workflow_id TEXT, 
    _function_num INT, 
    _params JSONB,
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
    SELECT dbos.check_execution(_workflow_uuid, _function_id) INTO return_value;
    if return_value IS NOT NULL THEN
        RETURN;
    END IF;

    ROLLBACK; 
    SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

    SELECT "triple_helix_demo_f"(_workflow_id, _params) INTO return_value;
    -- SELECT pg_current_snapshot(), pg_current_xact_id_if_assigned() INTO _snapshot, _txn_id;
    SELECT return_value::jsonb->'output', return_value::jsonb->'error' INTO _output, _error;

    IF _error IS NOT NULL OR jsonb_typeof(_error) <> 'null' THEN
        ROLLBACK;
    ELSE 
        INSERT INTO dbos.transaction_outputs (workflow_id, function_num, output) VALUES (_workflow_id, _function_num, _output);
        SELECT return_value::jsonb || jsonb_build_object('txn_snapshot', pg_current_snapshot(), 'txn_id', pg_current_xact_id_if_assigned()) INTO return_value;
    END IF;

END; $$;
