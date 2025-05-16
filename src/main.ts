import { DBOS } from "@dbos-inc/dbos-sdk";
import { NodeDataSource } from "./NodeDataSource";
import { PoolConfig } from "pg";

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

const stepsEvent = "steps_event";

async function sampleStep(step: number): Promise<number> {
  try {
    await sleep(1000);
    return step;
  } finally {
      console.log(`Completed step ${step}!`);
  }
}

async function sampleWorkflow(startValue: number): Promise<number> {
  let value = startValue;
  for (let i = 1; i < 5; i++) {
    value += await DBOS.runAsWorkflowStep(() => sampleStep(i), `sampleStep${i}`);
    await DBOS.setEvent(stepsEvent, i);
  }
  return value;
}

const sWF = DBOS.registerWorkflow(sampleWorkflow, { name: "sampleWorkflow" });

async function main() {
  const config: PoolConfig = { database: "triple_helix_app_db", user: "postgres" };
  await NodeDataSource.ensureDatabase(config, config.database!);
  await NodeDataSource.configure(config);
  

  // DBOS.setConfig({ "name": "triple-helix" });
  // await DBOS.launch();


  // try {
  //   let prevStep = 0;
  //   const workflowID = randomUUID();
  //   const handle = await DBOS.startWorkflowFunction({ workflowID }, sWF, 42);

  //   while (true) {
  //     const status = (await handle.getStatus())?.status;
  //     if (status === "SUCCESS" || status === "ERROR") {
  //       console.log(`Workflow status: ${status}`);
  //       break;
  //     }

  //     const event = await DBOS.getEvent<number>(workflowID, stepsEvent, 1);
  //     if (event && event !== prevStep) {
  //       console.log(`Workflow event: ${event}`);
  //       prevStep = event;
  //     } else {
  //       await sleep(500);
  //     }
  //   }

  //   const result = await handle.getResult();
  //   console.log(`Workflow completed with result: ${result}`);
  // } finally {
  //   await DBOS.shutdown();
  // }
}

main().catch(console.error);
