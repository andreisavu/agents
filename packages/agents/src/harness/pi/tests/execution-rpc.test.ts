import { env } from "cloudflare:workers";
import { describe, expect, it, vi, type TestContext } from "vitest";
import type { PiRpcHarnessTestObject } from "./worker";

function fresh(mode = "run") {
  return env.PI_RPC_HARNESS_TEST.getByName(`${mode}:${crypto.randomUUID()}`);
}

const hookError = "Pass schedule from the PiHarness factory to Harness.open";

async function requireHook(
  test: TestContext,
  box: ReturnType<typeof fresh>
): Promise<void> {
  const supported = await fresh("probe").piSupportsSchedule();
  test.skip(!supported, "Requires pi-durable with its awaitable schedule hook");
  expect(await box.rpcStartupError()).toBeUndefined();
}

function expectSingleModelPhase(
  phases: ReturnType<PiRpcHarnessTestObject["rpcPhases"]>
) {
  const token = phases[0]?.token;
  expect(token).toEqual(expect.any(String));
  expect(phases).toEqual([
    { phase: "model", boundary: "start", token, receiverActive: true },
    { phase: "model", boundary: "end", token, receiverActive: true }
  ]);
  return token;
}

// The pinned Pi release predates the hook. These cases run against the
// supporting release when installed; the diagnostic is tested on both.
describe("Pi execution through a native Durable Object RPC", () => {
  it("rejects a factory that does not forward schedule", async () => {
    expect(await fresh("missing-hook").rpcStartupError()).toContain(hookError);
  });

  it("runs model and tool phases in the self-RPC across concurrent admissions", async (test) => {
    const box = fresh();
    await requireHook(test, box);
    const session = await box.createSession();
    const [first, second] = await Promise.all([
      box.submit("multiply 4"),
      box.submit("multiply 5", { session })
    ]);
    const [one, two] = await Promise.all([
      box.wait(first.operationId),
      box.wait(second.operationId, session)
    ]);
    expect(one.status).toBe("done");
    expect(two.status).toBe("done");
    const phases = await box.rpcPhases();
    for (const boundary of ["start", "end"]) {
      const atBoundary = phases.filter((phase) => phase.boundary === boundary);
      expect(atBoundary.filter(({ phase }) => phase === "model")).toHaveLength(
        4
      );
      expect(atBoundary.filter(({ phase }) => phase === "tool")).toHaveLength(
        2
      );
    }
    for (const phase of phases) {
      expect(phase).toMatchObject({
        token: expect.any(String),
        receiverActive: true
      });
    }
    await vi.waitFor(async () => expect(await box.alarmTime()).toBeNull(), {
      timeout: 5_000
    });
  });

  it("retries a pre-entry RPC rejection from the persisted wake", async (test) => {
    const box = fresh();
    await requireHook(test, box);
    await vi.waitFor(async () => expect(await box.alarmTime()).toBeNull(), {
      timeout: 5_000
    });
    await box.failNextPiWork();
    const receipt = await box.submit("hello");
    const [failed] = await box.rpcFailures();
    expect(failed).toBeDefined();
    await box.runPiWork(failed!);
    expect((await box.wait(receipt.operationId)).status).toBe("done");
    expect(await box.rpcFailures()).toEqual([failed]);
    expect(expectSingleModelPhase(await box.rpcPhases())).not.toBe(failed);
  });

  it("clears alarm strikes after retrying a sender that never settles", async (test) => {
    const box = fresh();
    await requireHook(test, box);
    await vi.waitFor(async () => expect(await box.alarmTime()).toBeNull(), {
      timeout: 5_000
    });
    await box.failNextPiWork("drop-before-entry");
    await box.armRpcAlarmPrompt("gate");
    try {
      await vi.waitFor(async () => expect(await box.gateStarted()).toBe(true), {
        timeout: 10_000
      });
      expect(await box.rpcFailures()).toHaveLength(1);
      await box.seedMemoryLimitStrikes();
    } finally {
      await box.release();
    }
    await vi.waitFor(
      async () => {
        expect(await box.alarmTime()).toBeNull();
        expect(await box.memoryLimitStrikes()).toBe(0);
      },
      { timeout: 5_000 }
    );
  });

  it("keeps the receiver open when the sender loses its acknowledgement", async (test) => {
    const box = fresh();
    await requireHook(test, box);
    await vi.waitFor(async () => expect(await box.alarmTime()).toBeNull(), {
      timeout: 5_000
    });
    await box.failNextPiWork("after-entry");
    const held = await box.submit("hold");
    await vi.waitFor(async () => expect(await box.holding()).toBe(true));
    const toolStart = (await box.rpcPhases()).find(
      ({ phase, boundary }) => phase === "tool" && boundary === "start"
    );
    try {
      await box.rejectPiWorkSender();
      expect(toolStart).toMatchObject({
        token: expect.any(String),
        receiverActive: true
      });
      // Late redelivery must return without taking over the active work.
      await box.runPiWork(toolStart!.token!);
    } finally {
      await box.releaseHold();
    }
    expect((await box.wait(held.operationId)).status).toBe("done");
    expect(
      (await box.rpcPhases()).filter(({ phase }) => phase === "tool")
    ).toEqual([
      toolStart,
      { ...toolStart, boundary: "end", receiverActive: true }
    ]);
  });

  it("rejects a startup prompt before admission and runs a startup submission", async (test) => {
    const box = fresh("startup");
    await requireHook(test, box);
    const { error, receipt } = await box.rpcStartup();
    expect(error).toContain("cannot wait for work during startup");
    expect(receipt).toBeDefined();
    expect((await box.wait(receipt!.operationId)).text).toBe(
      "echo: from startup"
    );
    expectSingleModelPhase(await box.rpcPhases());
  });

  it("withdraws queued work during startup and rejects conversation aborts", async (test) => {
    const box = fresh("startup-abort");
    await requireHook(test, box);
    const guard = expect.stringContaining(
      "cannot wait for work during startup"
    );
    expect(await box.rpcStartupAborts()).toEqual({
      empty: guard,
      whole: guard,
      placed: guard,
      queued: true
    });
    expect(await box.wait("placed")).toMatchObject({
      status: "done",
      text: "echo: first"
    });
    expect(await box.wait("queued")).toMatchObject({
      status: "unanswered",
      reason: "aborted"
    });
    expectSingleModelPhase(await box.rpcPhases());
  });

  it("runs new work after dispose reports a storage close failure", async (test) => {
    const box = fresh();
    await requireHook(test, box);
    const before = await box.submit("before close");
    expect((await box.wait(before.operationId)).text).toBe(
      "echo: before close"
    );
    expect(await box.disposeThroughStorageError()).toContain(
      "injected Pi storage close failure"
    );
    const after = await box.submit("after close");
    expect((await box.wait(after.operationId)).text).toBe("echo: after close");
  });

  it("keeps the receiver open through graceful disposal", async (test) => {
    const box = fresh();
    await requireHook(test, box);
    await box.submit("hold");
    await vi.waitFor(async () => expect(await box.holding()).toBe(true));
    expect(await box.disposeHeldTool()).toBe(true);
  });

  it("resumes a timed sleep beside a tool that stays active", async (test) => {
    const box = fresh();
    await requireHook(test, box);
    const held = await box.submit("hold");
    await vi.waitFor(async () => expect(await box.holding()).toBe(true));
    try {
      await box.remindAt(Date.now() + 500);
      expect(await box.liveReminders()).toBe(1);
      await vi.waitFor(async () => expect(await box.liveReminders()).toBe(0), {
        timeout: 5_000
      });
    } finally {
      await box.releaseHold();
    }
    expect((await box.wait(held.operationId)).status).toBe("done");
  });

  it("allows a host alarm handler to await a Pi prompt", async (test) => {
    const box = fresh();
    await requireHook(test, box);
    await box.armRpcAlarmPrompt();
    await vi.waitFor(
      async () => expect(await box.rpcAlarmResult()).toBe("echo: from onAlarm"),
      { timeout: 5_000 }
    );
    expectSingleModelPhase(await box.rpcPhases());
  });

  it("runs restart-only cleanup after closing and reopening idle Pi", async (test) => {
    const box = fresh();
    await requireHook(test, box);
    const task = await box.restartOnlyReminder();
    await vi.waitFor(
      async () => {
        expect(await box.reminderState(task)).toBe("sleeping");
        expect(await box.alarmTime()).toBeNull();
      },
      { timeout: 5_000 }
    );
    await Promise.all([box.dispose(), box.reopen()]);
    await vi.waitFor(
      async () => expect(await box.reminderState(task)).toBe("aborted"),
      {
        timeout: 5_000
      }
    );
  });
});
