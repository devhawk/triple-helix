// import { main as pg_main } from './test-postgres.ts'
// import { main as npg_main } from './test-node-postgres.ts'

import { setupDB } from "./setup-db.ts";

async function main() {
    // ensure the database is created and configured
    const config = { database: "triple_helix_app_db", user: "postgres" };
    await setupDB(config);

    const argv = process.argv.slice(2);
    const arg1 = argv.length > 0 ? argv[0] : "npg";

    if (arg1 === "npg") {
        const npg = await import("./test-node-postgres.ts")
        console.log("Running test-node-postgres");
        await npg.main();
    } else if (arg1 === "pg") {
        const pg = await import("./test-postgres.ts")
        console.log("Running test-postgres");
        await pg.main();
    } else {
        console.error(`Unknown argument: ${arg1}`);
    }
}

main().catch(console.error);