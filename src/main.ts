import { DBOS } from "@dbos-inc/dbos-sdk";

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

const stepsEvent = "steps_event";

export class Example {
  @DBOS.step()
  static async stepOne() {
    await sleep(1000);
    console.log("Completed step 1!")
  }

  @DBOS.step()
  static async stepTwo() {
    await sleep(1000);
    console.log("Completed step 2!")
  }

  @DBOS.step()
  static async stepThree() {
    await sleep(1000);
    console.log("Completed step 3!")
  }

  @DBOS.workflow()
  static async workflow(): Promise<void> {
    await Example.stepOne();
    await DBOS.setEvent(stepsEvent, 1);
    await Example.stepTwo();
    await DBOS.setEvent(stepsEvent, 2);
    await Example.stepThree();
    await DBOS.setEvent(stepsEvent, 3);
  }
}

function generateRandomString() {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  return Array.from(crypto.getRandomValues(new Uint8Array(6)))
    .map((x) => chars[x % chars.length])
    .join("");
}

async function main() {
  DBOS.setConfig({
    "name": "triple-helix",
    "databaseUrl": process.env.DBOS_DATABASE_URL
  });
  await DBOS.launch();
  try {
    const workflowID = generateRandomString();
    const handle = await DBOS.startWorkflow(Example, { workflowID }).workflow();

    let previousStep;
    while (true) {
      const step = await DBOS.getEvent<number>(handle.workflowID, stepsEvent, 2);
      if (step) {
        if (step === previousStep) {
          await sleep(500);
          continue;
        } else {
          previousStep = step;
          const status = (await handle.getStatus())?.status;
          console.log(`Workflow ${handle.workflowID} step ${step} status: ${status}`);
          if (status === "SUCCESS") {
            break;
          }
        }
      }
    }
  } finally {
    await DBOS.shutdown();
  }
}

main().catch(console.error);
