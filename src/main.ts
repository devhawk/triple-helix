import { DBOS } from "@dbos-inc/dbos-sdk";
import { IsolationLevel, PostgresDataSource as PGDS, PostgresDataSource } from "./PostgresDataSource.ts";
import { randomUUID } from "node:crypto";

// configure the app DB data source
const config = { database: "triple_helix_app_db", user: "postgres" };
const dataSource = new PGDS("app-db", config);
DBOS.registerDataSource(dataSource);

// helper sleep function
function sleep(ms: number): Promise<void> { return new Promise(resolve => setTimeout(resolve, ms)); }

// global steps event name so it can be accessed in workflow and main
const stepsEvent = "steps_event";

// our sample step function
async function sampleStep(step: number): Promise<number> {
  try {
    await sleep(1000);
    return step;
  } finally {
      console.log(`Completed step ${step}!`);
  }
}

// registered version of sampleStep
const registeredSampleStep = DBOS.registerStep(sampleStep, { name: "sampleStep" });

// our sample transaction function
async function sampleTxStep(step: number): Promise<number> {
  type Result = { step: number };
  const result = await PGDS.client<Result[]>`SELECT ${step}::int AS step`;
  return result[0].step;
}

// registered version of sampleTxStep
const registeredSampleTxStep = dataSource.register(sampleTxStep, "sampleTxStep", { isolationLevel: IsolationLevel.repeatableRead });

// our sample workflow function
async function sampleWorkflow(startValue: number): Promise<number> {
  let value = startValue;
  for (let i = 1; i < 5; i++) {
    // run using the registered step and transaction functions
    value += await registeredSampleStep(i);
    value += await registeredSampleTxStep(i);

    // run step and tx via runAs static methods
    value += await DBOS.runAsWorkflowStep(() => sampleStep(i), "sampleStep");
    // run tx using DBOS static method (not type safe)
    value += await DBOS.runAsWorkflowTransaction(() => sampleTxStep(i), "sampleTxStep", { dsName: dataSource.name, config: { isolationLevel: IsolationLevel.repeatableRead }});
    // run tx using PostgresDataSource static method (type safe)
    value += await PostgresDataSource.runAsWorkflowTransaction(() => sampleTxStep(i), "sampleTxStep", { dsName: dataSource.name, config: { isolationLevel: IsolationLevel.repeatableRead }});
    // run tx using dataSource instance method (type safe + no dsName)
    value += await dataSource.runAsWorkflowTransaction(() => sampleTxStep(i), "sampleTxStep", { isolationLevel: IsolationLevel.repeatableRead });

    // communicate progress via event
    await DBOS.setEvent(stepsEvent, i);
  }
  return value;
}

// registered version of sampleWorkflow
const registeredSampleWorkflow = DBOS.registerWorkflow(sampleWorkflow, { name: "sampleWorkflow" });

async function main() {
  // ensure the database is created and configured
  await PGDS.ensureDatabase(config.database, { ...config, database: "postgres" });
  await PGDS.configure(config);

  // launch DBOS
  DBOS.setConfig({ "name": "triple-helix" });
  await DBOS.launch();

  try {
    // run the workflow
    const workflowID = randomUUID();
    const handle = await DBOS.startWorkflowFunction({ workflowID }, registeredSampleWorkflow, 42);

    // log workflow events to the console
    let prevStep = 0;
    while (true) {
      const status = (await handle.getStatus())?.status;
      if (status === "SUCCESS" || status === "ERROR") {
        console.log(`Workflow status: ${status}`);
        break;
      }

      const event = await DBOS.getEvent<number>(workflowID, stepsEvent, 1);
      if (event && event !== prevStep) {
        console.log(`Workflow event: ${event}`);
        prevStep = event;
      } else {
        await sleep(500);
      }
    }

    // getting the workflow result should be a no-op since the workflow is already completed
    const result = await handle.getResult();
    console.log(`Workflow completed with result: ${result}`);
  } finally {
    await DBOS.shutdown();
  }
}

main().catch(console.error);
