import { DBOS } from "@dbos-inc/dbos-sdk";
import { IsolationLevel, NodePostgresDataSource as NPGDS } from "./NodePostgresDataSource.ts";
import { randomUUID } from "node:crypto";

// configure the app DB data source
const config = { database: "triple_helix_app_db", user: "postgres" };
const dataSource = new NPGDS("app-db", config);
DBOS.registerDataSource(dataSource);

// helper sleep function
function sleep(ms: number): Promise<void> { return new Promise(resolve => setTimeout(resolve, ms)); }

// global steps event name so it can be accessed in workflow and main
const stepsEvent = "steps_event";

// query result type reused across multiple sample tx step functions
type StepQueryResult = { step: number };

// a sample step function
async function sampleStep(step: number): Promise<number> {
  try {
    await sleep(1000);
    return step;
  } finally {
    DBOS.logger.info(`Completed sampleStep ${step}!`);
  }
}

// a sample transaction function
async function sampleTxStep(step: number): Promise<number> {
  try {
    const result = await NPGDS.client.query<StepQueryResult>(`SELECT $1::int AS step`, [step]);
    return result.rows[0].step;
  } finally {
    DBOS.logger.info(`Completed sampleTxStep ${step}!`);
  }
}

// registered versions of sampleStep & sampleTxStep
const registeredSampleStep = DBOS.registerStep(sampleStep, { name: "sampleStep" });
const registeredSampleTxStep = dataSource.register(sampleTxStep, "sampleTxStep", { storedProc: "triple_helix_demo" });

// a class to demonstrate static step and transaction functions
class StaticStep {
  static count = 0;
  static async sampleStep(step: number): Promise<number> {
    try {
      StaticStep.count++;
      await sleep(1000);
      return step;
    } finally {
      DBOS.logger.info(`Completed StaticStep.sampleStep ${step}!`);
    }
  }

  static async sampleTxStep(step: number): Promise<number> {
    try {
      StaticStep.count++;
      const result = await NPGDS.client.query<StepQueryResult>(`SELECT $1::int AS step`, [step]);
      return result.rows[0].step;
    } finally {
      DBOS.logger.info(`Completed StaticStep.sampleTxStep ${step}!`);
    }
  }
}

// register static step functions w/o decorators
StaticStep.sampleStep = DBOS.registerStep(StaticStep.sampleStep, { name: "StaticStep.sampleStep" });
StaticStep.sampleTxStep = dataSource.register(StaticStep.sampleTxStep, "StaticStep.sampleTxStep", { storedProc: "triple_helix_demo" });

// a class to demonstrate instance step and transaction functions
class InstanceStep {
  count = 0;
  async sampleStep(step: number): Promise<number> {
    try {
      this.count++;
      await sleep(1000);
      return step;
    } finally {
      DBOS.logger.info(`Completed InstanceStep.sampleStep ${step}!`);
    }
  }

  async sampleTxStep(step: number): Promise<number> {
    try {
      this.count++;
      const result = await NPGDS.client.query<StepQueryResult>(`SELECT $1::int AS step`, [step]);
      return result.rows[0].step;
    } finally {
      DBOS.logger.info(`Completed InstanceStep.sampleTxStep ${step}!`);
    }
  }
}

// register instance step functions w/o decorators
InstanceStep.prototype.sampleStep = DBOS.registerStep(InstanceStep.prototype.sampleStep, { name: "InstanceStep.sampleStep" });
InstanceStep.prototype.sampleTxStep = dataSource.register(InstanceStep.prototype.sampleTxStep, "InstanceStep.sampleTxStep", { storedProc: "triple_helix_demo" });

// a sample workflow function
async function sampleWorkflow(startValue: number): Promise<number> {
  let value = startValue;
  let instance = new InstanceStep();

  for (let i = 1; i < 2; i++) {
    // run using the registered step and transaction functions
    value += await registeredSampleStep(i);
    value += await registeredSampleTxStep(i);
    value += await StaticStep.sampleStep(i);
    value += await StaticStep.sampleTxStep(i);
    value += await instance.sampleStep(i);
    value += await instance.sampleTxStep(i);

    // run non transactional step via DBOS static method
    value += await DBOS.runAsWorkflowStep(async () => {
      try {
        await sleep(1000);
        return i;
      } finally {
        DBOS.logger.info(`Completed DBOS.runAsWorkflowStep ${i}!`);
      }
    }, "DBOS.runAsWorkflowStep");

    // run tx step via DBOS static method (have to specify DS name, config not type safe)
    value += await DBOS.runAsWorkflowTransaction(async () => {
      try {
        const result = await NPGDS.client.query<StepQueryResult>(`SELECT $1::int AS step`, [i]);
        return result.rows[0].step;
      } finally {
        DBOS.logger.info(`Completed DBOS.runAsWorkflowTransaction ${i}!`);
      }
    }, "DBOS.runAsWorkflowTransaction", { dsName: dataSource.name });

    // run tx step using PostgresDataSource static method (have to specify DS name, config type safe)
    value += await NPGDS.runTxStep(async () => {
      try {
        const result = await NPGDS.client.query<StepQueryResult>(`SELECT $1::int AS step`, [i]);
        return result.rows[0].step;
      } finally {
        DBOS.logger.info(`Completed PostgresDataSource.runTxStep ${i}!`);
      }
    }, "PostgresDataSource.runTxStep", { dsName: dataSource.name });

    // run tx step using PostgresDataSource instance method (don't specify DS name, config type safe)
    value += await dataSource.runTxStep(async () => {
      try {
        const result = await NPGDS.client.query<StepQueryResult>(`SELECT $1::int AS step`, [i]);
        return result.rows[0].step;
      } finally {
        DBOS.logger.info(`Completed dataSource.runTxStep ${i}!`);
      }
    }, "dataSource.runTxStep", { isolationLevel: IsolationLevel.serializable });

    // communicate progress via event
    await DBOS.setEvent(stepsEvent, i);
  }

  DBOS.logger.info(`StaticStep count: ${StaticStep.count}`);
  DBOS.logger.info(`InstanceStep count: ${instance.count}`);
  return value;
}

// registered version of sampleWorkflow
const registeredSampleWorkflow = DBOS.registerWorkflow(sampleWorkflow, { name: "sampleWorkflow" });

export async function main() {

  // launch DBOS
  DBOS.setConfig({ "name": "triple-helix" });
  await DBOS.launch();

  try {
    // run the workflow
    const workflowID = randomUUID();
    const handle = await DBOS.startWorkflowFunction({ workflowID }, registeredSampleWorkflow, 0);

    // log workflow events to the console
    let prevStep = 0;
    while (true) {
      const status = (await handle.getStatus())?.status;
      if (status === "SUCCESS" || status === "ERROR") {
        DBOS.logger.info(`Workflow status: ${status}`);
        break;
      }

      const event = await DBOS.getEvent<number>(workflowID, stepsEvent, 1);
      if (event && event !== prevStep) {
        DBOS.logger.info(`Workflow event: ${event}`);
        prevStep = event;
      } else {
        await sleep(500);
      }
    }

    // getting the workflow result should be a no-op since the workflow is already completed
    const result = await handle.getResult();
    DBOS.logger.info(`Workflow completed with result: ${result}`);
  } finally {
    await DBOS.shutdown();
  }
}
