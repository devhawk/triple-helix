// using https://github.com/brianc/node-postgres

import { DBOS, type DBOSTransactionalDataSource } from "@dbos-inc/dbos-sdk";
import { Client, ClientBase, type ClientConfig, DatabaseError, Pool, type PoolConfig } from "pg";
import { AsyncLocalStorage } from "node:async_hooks";

export const IsolationLevel = Object.freeze({
    serializable: 'SERIALIZABLE',
    repeatableRead: 'REPEATABLE READ',
    readCommited: 'READ COMMITTED',
    readUncommitted: 'READ UNCOMMITTED',
});

type ValuesOf<T> = T[keyof T];
type IsolationLevel = ValuesOf<typeof IsolationLevel>;

export interface NodePostgresTransactionOptions {
    isolationLevel?: IsolationLevel;
};

interface NodePostgresDataSourceContext { client: ClientBase; }
const asyncLocalCtx = new AsyncLocalStorage<NodePostgresDataSourceContext>();

export class NodePostgresDataSource implements DBOSTransactionalDataSource {
    readonly name: string;
    readonly #pool: Pool;

    constructor(name: string, config: PoolConfig) {
        this.name = name;
        this.#pool = new Pool(config);
    }

    static get client(): ClientBase {
        if (!DBOS.isInTransaction()) { throw new Error("invalid use of PostgresDataSource.client outside of a DBOS transaction."); }
        const ctx = asyncLocalCtx.getStore();
        if (!ctx) { throw new Error("No async local context found."); }
        return ctx.client;
    }

    static async runTxStep<T>(callback: () => Promise<T>, funcName: string, options: { dsName?: string, config?: NodePostgresTransactionOptions } = {}) {
        return await DBOS.runAsWorkflowTransaction(callback, funcName, options);
    }

    async runTxStep<T>(callback: () => Promise<T>, funcName: string, config?: NodePostgresTransactionOptions) {
        return await DBOS.runAsWorkflowTransaction(callback, funcName, { dsName: this.name, config });
    }

    register<This, Args extends unknown[], Return>(
        func: (this: This, ...args: Args) => Promise<Return>,
        name: string,
        config?: NodePostgresTransactionOptions
    ): (this: This, ...args: Args) => Promise<Return> {
        return DBOS.registerTransaction(this.name, func, { name }, config);
    }

    get dsType(): string {
        return "NodePostgresDataSource";
    }

    initialize(): Promise<void> {
        return Promise.resolve();
    }

    destroy(): Promise<void> {
        return this.#pool.end();
    }

    async #getResult(workflowID: string, functionNum: number): Promise<string | undefined> {
        type Result = { output: string };
        const result = await this.#pool.query<Result>(/*sql*/
            `SELECT output FROM dbos.transaction_outputs
             WHERE workflow_id = $1 AND function_num = $2`,
            [workflowID, functionNum]);
        return result.rows.length > 0
            ? result.rows[0].output
            : undefined;
    }

    async invokeTransactionFunction<This, Args extends unknown[], Return>(
        config: NodePostgresTransactionOptions,
        target: This,
        func: (this: This, ...args: Args) => Promise<Return>,
        ...args: Args
    ): Promise<Return> {
        const workflowID = DBOS.workflowID;
        const functionNum = DBOS.stepID;
        const isolationLevel = config.isolationLevel ? /*sql*/`ISOLATION LEVEL ${config.isolationLevel}` : "";

        if (!workflowID) { throw new Error("Workflow ID is not set."); }
        if (!functionNum) { throw new Error("Function Number is not set."); }

        while (true) {
            const result = await this.#getResult(workflowID, functionNum);
            if (result) { return JSON.parse(result) as Return; }

            const client = await this.#pool.connect();
            try {
                await client.query(/*sql*/`BEGIN ${isolationLevel}`);

                const output = await asyncLocalCtx.run({ client }, async () => {
                    return await func.call(target, ...args);
                });

                try {
                    await client.query(/*sql*/
                        `INSERT INTO dbos.transaction_outputs (workflow_id, function_num, output) VALUES ($1, $2, $3)`,
                        [workflowID, functionNum, JSON.stringify(output)]);
                } catch (error) {
                    // 23505 is a duplicate key error 
                    if (error instanceof DatabaseError && error.code === "23505") {
                        await client.query(/*sql*/`ROLLBACK`);
                        continue;
                    } else {
                        throw error;
                    }
                }

                await client.query(/*sql*/`COMMIT`);
                return output;
            } catch (error) {
                await client.query(/*sql*/`ROLLBACK`);
                throw error;
            } finally {
                client.release();
            }
        }
    }

        static async ensureDatabase(name: string, config: ClientConfig): Promise<void> {
            const client = new Client(config);
            try {
                await ensureDB(client, name);
            } finally {
                await client.end();
            }
        }
    
        static async configure(config: ClientConfig): Promise<void> {
            const client = new Client(config);
            try {
                await client.query(/*sql*/
                    `CREATE SCHEMA IF NOT EXISTS dbos;
                     CREATE TABLE IF NOT EXISTS dbos.transaction_outputs (
                        workflow_id TEXT NOT NULL,
                        function_num INT NOT NULL,
                        output TEXT,
                        created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint,
                        PRIMARY KEY (workflow_id, function_num));`);
            } finally {
                await client.end();
            }
        }
    
}

// helper functions to create/drop the database
async function checkDB(sql: ClientBase, name: string) {
    const results = await sql.query(/*sql*/`SELECT 1 FROM pg_database WHERE datname = $1`, [name]);
    return results.rows.length > 0;
}

export async function ensureDB(sql: ClientBase, name: string) {
    const exists = await checkDB(sql, name);
    if (!exists) {
        await sql.query(/*sql*/ `CREATE DATABASE ${name}`);
    }
}

export async function dropDB(sql: ClientBase, name: string) {
    const exists = await checkDB(sql, name);
    if (exists) {
        await sql.query(/*sql*/ `DROP DATABASE ${name}`);
    }
}