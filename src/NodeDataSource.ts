import { DBOSTransactionalDataSource } from "@dbos-inc/dbos-sdk";
import { MethodRegistration } from "@dbos-inc/dbos-sdk/dist/src/decorators.js";
import { Pool, PoolConfig, Client, ClientConfig } from "pg";

async function checkDB(client: Client, name: string): Promise<boolean> {
    const result = await client.query<{exists: boolean}>("SELECT EXISTS (SELECT FROM pg_database WHERE datname = $1)", [name]);
    return result.rows[0].exists;
}

async function ensureDB(client: Client, name: string): Promise<void> {
    const exists = await checkDB(client, name);
    if (!exists) {
        await client.query(`CREATE DATABASE ${name}`);
    }
}

export class NodeDataSource implements DBOSTransactionalDataSource {

    constructor(name: string, config: PoolConfig) {
        this.name = name;
        this.#config = config;
    }

    readonly name: string;
    readonly #config: PoolConfig;
    #pool: Pool | undefined;

    get dsType(): string { 
        return "NodeDataSource";
    }

    initialize(): Promise<void> {
        const pool = new Pool(this.#config);
        this.#pool = pool;
        return Promise.resolve();
    }

    destroy(): Promise<void> {
        const pool = this.#pool;
        this.#pool = undefined;
        return pool?.end() ?? Promise.resolve();
    }

    async invokeTransactionFunction<This, Args extends unknown[], Return>(
        reg: MethodRegistration<This, Args, Return> | undefined, 
        config: unknown, 
        target: This, 
        func: (this: This, ...args: Args) => Promise<Return>, 
        ...args: Args
    ): Promise<Return> {
        throw new Error("Method not implemented.");
    }

    static async ensureDatabase(config: ClientConfig, name: string): Promise<void> {
        const client = new Client({ ...config, database: "postgres" });
        try {
            await client.connect();
            await ensureDB(client, name);
        } finally {
            await client.end();
        }
    }

    static async configure(config: ClientConfig): Promise<void> {
        const client = new Client(config);
        try {
            await client.connect();
            await client.query("CREATE SCHEMA IF NOT EXISTS dbos;");
            await client.query(`CREATE TABLE IF NOT EXISTS dbos.transaction_outputs (
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