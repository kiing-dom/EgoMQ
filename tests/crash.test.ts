import { describe, it, expect, beforeEach, vi, afterEach } from "vitest";
import { existsSync, unlinkSync } from "node:fs";
import { Queue } from "../src/queue";
import { send } from "node:process";

const TEST_DB = "./tests/test-crash-recovery.db";
const schemaPath = "./schema.sql"
const sendEmail = "send_email";

beforeEach(() => {
    if (existsSync(TEST_DB)) unlinkSync(TEST_DB);
});

afterEach(() => {
    if (existsSync(TEST_DB)) unlinkSync(TEST_DB);
});

describe("queue crash recovery", () => {
    it("recovers a job stuck in 'running' to pending on restart", async () => {
        vi.useFakeTimers();

        const queue1 = new Queue(TEST_DB, schemaPath);
        const jobId = await queue1.enqueue(sendEmail, {});
        queue1._setStatusForTest(jobId, 'running');
        queue1.close();

        // simulating a restart by using a fresh instance w/ same DB
        const queue2 = new Queue(TEST_DB, schemaPath);
        const job = queue2.getJob(jobId);
        queue2.close();

        expect(job).toBeDefined();
        expect(job?.status).toBe('pending');

        vi.useRealTimers();
    });

    it("does not touch jobs that are genuinely still pending or completed", async () => {
        vi.useFakeTimers();

        const queue1 = new Queue(TEST_DB, schemaPath);
        const pendingId = await queue1.enqueue(sendEmail, {});
        const completedId = await queue1.enqueue(sendEmail, {});
        queue1._setStatusForTest(completedId, "completed");
        queue1.close()

        const queue2 = new Queue(TEST_DB, schemaPath);
        expect(queue2.getJob(pendingId)?.status).toBe("pending");
        expect(queue2.getJob(completedId)?.status).toBe("completed");
        queue2.close();

        vi.useRealTimers();
    });
    
    it("a recovered job actually gets reprocessed by start()", async () => {
        vi.useFakeTimers();

        const queue1 = new Queue(TEST_DB, schemaPath);
        const jobId = await queue1.enqueue(sendEmail, {});
        queue1._setStatusForTest(jobId, "running"); // simulating crash
        queue1.close();

        const queue2 = new Queue(TEST_DB, schemaPath);
        queue2.register(sendEmail, async () => {});
        const startPromise = queue2.start();
    
        vi.runAllTimersAsync();
        await startPromise;

        expect(queue2.getJob(jobId)?.status).toBe("completed");
        
        queue2.close();
        vi.useRealTimers();
    });
});