import { describe, it, expect, vi } from "vitest";
import { Queue } from "../src/queue";

const TEST_DB = ":memory:";
const schemaPath = "./schema.sql"
const sendEmail = "send_email";

describe("queue processNext / start", () => {
    it ("completes a job that succeeds on the first try", async () => {
        vi.useFakeTimers();

        const queue = new Queue(TEST_DB, schemaPath);
        const jobId = await queue.enqueue(sendEmail, {});
        const handler = vi.fn()
        queue.register(sendEmail, handler);
        
        await queue.processNext();
        
        vi.runAllTimersAsync();
        
        const job = queue.getJob(jobId);
        expect(job?.status).toBe("completed");

        vi.useRealTimers();
    });

    it ("stops start() when there are no more pending or running jobs", async () => {
        vi.useFakeTimers();

        const queue = new Queue(TEST_DB, schemaPath);
        const id1 = await queue.enqueue(sendEmail, {});
        const id2 = await queue.enqueue(sendEmail, {});
        const handler = vi.fn();
        queue.register(sendEmail, handler);

        await queue.start();

        const job1 = queue.getJob(id1);
        const job2 = queue.getJob(id2);

        expect(job1?.status).toBe("completed");        
        expect(job2?.status).toBe("completed");

        const next = await queue.processNext();

        expect(next).toBeUndefined();
    });

    it ("picks the due job over one scheduled for later", async () => {
        vi.useFakeTimers();

        const queue = new Queue(TEST_DB, schemaPath);
        const handler = vi.fn();
        queue.register(sendEmail, handler)

        vi.runAllTimersAsync();
        const futureId = await queue.enqueue(sendEmail, { order: "later" });
        queue._setRunAtForTest(futureId, new Date(Date.now() + 60_000));
        const nowId = await queue.enqueue(sendEmail, { order: "now" });

        const processed = await queue.processNext();
        expect(processed?.id).toBe(nowId);

        const futureJob = queue.getJob(futureId);
        expect(futureJob?.status).toBe("pending");

        const next = await queue.processNext();
        expect(next).toBeUndefined();

        vi.useRealTimers();
    });

    it("retries a failing job with backoff befre eventually completing", async () => {
        vi.useFakeTimers();

        const queue = new Queue(TEST_DB, schemaPath);
        const handler = vi.fn()
            .mockRejectedValueOnce(new Error("mega fail!"))
            .mockResolvedValueOnce(undefined);

        queue.register(sendEmail, handler);

        const jobId = await queue.enqueue(sendEmail, { to: "a@b.com" }, { maxRetries: 3} );

        const first = await queue.processNext();
        expect(first?.id).toBe(jobId);
        expect(handler).toHaveBeenCalledOnce();

        let job = queue.getJob(jobId);
        expect(job?.status).toBe("pending");
        expect(job?.retries).toBe(1);
        expect(job?.error).toContain("mega fail!");
        expect(job?.runAt).toBeDefined();
        expect(job?.runAt!.getTime()).toBeGreaterThan(Date.now());

        const tooSoon = await queue.processNext();
        expect(tooSoon).toBeUndefined();
        expect(handler).toHaveBeenCalledOnce();

        queue._setRunAtForTest(jobId, new Date(Date.now() - 1000));
        const second = await queue.processNext();
        expect(second?.id).toBe(jobId);
        expect(handler).toHaveBeenCalledTimes(2);

        job = queue.getJob(jobId);
        expect(job?.status).toBe("completed");
        expect(job?.error).toBeUndefined();
    });
}) 