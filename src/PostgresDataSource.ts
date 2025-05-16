import postgres, { type Sql } from "postgres";
import { DBOS, type DBOSTransactionalDataSource } from "@dbos-inc/dbos-sdk";
import { AsyncLocalStorage } from "node:async_hooks";

async function checkDB(sql: Sql, name: string) {
    const results =
        await sql/*sql*/ `SELECT 1 FROM pg_database WHERE datname = ${name}`.values();
    return results.length > 0;
}

export async function ensureDB(sql: Sql, name: string) {
    const exists = await checkDB(sql, name);
    if (!exists) {
        await sql.unsafe(/*sql*/ `CREATE DATABASE ${name}`).simple();
    }
}

export async function dropDB(sql: Sql, name: string) {
    const exists = await checkDB(sql, name);
    if (exists) {
        await sql.unsafe(/*sql*/ `DROP DATABASE ${name}`).simple();
    }
}

interface PostgresDataSourceContext { client: postgres.TransactionSql<{}>; }
const asyncLocalCtx = new AsyncLocalStorage<PostgresDataSourceContext>();

class TxOutputDuplicateKeyError extends Error { }

export const IsolationLevel = Object.freeze({
    serializable: Symbol("serializable"),
    repeatableRead: Symbol("repeatableRead"),
    readCommited: Symbol("readCommited"),
    readUncommitted: Symbol("readUncommitted"),
});

function getIsolationLevel(isolationLevel: Symbol | undefined): string {

    const $isolationLevel = (function (isolationLevel: Symbol | undefined) {
        switch (isolationLevel) {
            case IsolationLevel.serializable:
                return "SERIALIZABLE";
            case IsolationLevel.repeatableRead:
                return "REPEATABLE READ";
            case IsolationLevel.readUncommitted:
                return "READ UNCOMMITTED";
            case IsolationLevel.readCommited:
                return "READ COMMITTED";
            case undefined:
                return undefined;
            default:
                throw new Error(`Invalid isolation level: ${isolationLevel}`);
        }
    })(isolationLevel);

    return $isolationLevel ? `ISOLATION LEVEL ${$isolationLevel}` : "";
}

export interface PostgresTransactionOptions {
    isolationLevel?: Symbol;
};

export class PostgresDataSource implements DBOSTransactionalDataSource {

    readonly name: string;
    readonly #db: Sql;

    constructor(name: string, options: postgres.Options<{}> = {}) {
        this.name = name;
        this.#db = postgres(options);
    }

    static get client(): postgres.TransactionSql<{}> {
        if (!DBOS.isInTransaction()) { throw new Error("invalid use of PostgresDataSource.client outside of a DBOS transaction."); }
        const ctx = asyncLocalCtx.getStore();
        if (!ctx) { throw new Error("No async local context found."); }
        return ctx.client;
    }

    static async runAsWorkflowTransaction<T>(callback: () => Promise<T>, funcName: string, options: { dsName?: string, config?: PostgresTransactionOptions } = {}) {
        return await DBOS.runAsWorkflowTransaction(callback, funcName, options);
    }

    get dsType(): string {
        return "PostgresDataSource";
    }

    initialize(): Promise<void> {
        return Promise.resolve();
    }

    destroy(): Promise<void> {
        return this.#db.end();
    }

    async #getResult(workflowID: string, functionNum: number): Promise<string | undefined> {
        type Result = { output: string };
        const result = await this.#db<Result[]>/*sql*/`
            SELECT output FROM dbos.transaction_outputs
            WHERE workflow_id = ${workflowID} AND function_num = ${functionNum}`;
        return result[0]?.output;
    }

    async invokeTransactionFunction<This, Args extends unknown[], Return>(
        config: PostgresTransactionOptions,
        target: This,
        func: (this: This, ...args: Args) => Promise<Return>,
        ...args: Args
    ): Promise<Return> {
        const workflowID = DBOS.workflowID;
        if (!workflowID) { throw new Error("Workflow ID is not set."); }
        const functionNum = DBOS.stepID;
        if (!functionNum) { throw new Error("Function Number is not set."); }

        const isolationLevel = getIsolationLevel(config.isolationLevel);

        while (true) {
            const result = await this.#getResult(workflowID, functionNum);
            if (result) { return JSON.parse(result) as Return; }

            try {
                const output = await this.#db.begin<Return>(isolationLevel, async (tx) => {
                    const output = await asyncLocalCtx.run({ client: tx }, async () => {
                        return await func.call(target, ...args);
                    });

                    try {
                        await tx/*sql*/`
                            INSERT INTO dbos.transaction_outputs (workflow_id, function_num, output)
                            VALUES (${workflowID}, ${functionNum}, ${JSON.stringify(output)})`;
                    } catch (e) {
                        if (e instanceof postgres.PostgresError && e.code === "23505") {
                            throw new TxOutputDuplicateKeyError("Duplicate key error");
                        } else {
                            throw e;
                        }
                    }

                    return output;
                });

                return output as Return
            } catch (e) {
                if (e instanceof TxOutputDuplicateKeyError) {
                    continue;
                } else {
                    throw e;
                }
            }
        }
    }

    static async ensureDatabase(name: string, options: postgres.Options<{}> = {}): Promise<void> {
        const pg = postgres({ ...options, onnotice: () => { } });
        try {
            await ensureDB(pg, name);
        } finally {
            await pg.end();
        }
    }

    static async configure(options: postgres.Options<{}> = {}): Promise<void> {
        const pg = postgres({ ...options, onnotice: () => { } });
        try {
            await pg/*sql*/`
                CREATE SCHEMA IF NOT EXISTS dbos;
                CREATE TABLE IF NOT EXISTS dbos.transaction_outputs (
                    workflow_id TEXT NOT NULL,
                    function_num INT NOT NULL,
                    output TEXT,
                    created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint,
                    PRIMARY KEY (workflow_id, function_num));`.simple();
        } finally {
            await pg.end();
        }
    }
}