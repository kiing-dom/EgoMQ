import { describe, it, expect } from "vitest";
import { Queue } from "../src/queue";

const TEST_DB = ":memory:";
const schemaPath = "./schema.sql";

describe("enqueue", () => {
    it ("writes a job with the correct shape to the DB", async () => {
        const queue = new Queue(TEST_DB, schemaPath);
        const jobId = await queue.enqueue("send_email", { to: "me@email.com"});
        const job = queue.getJob(jobId);

        expect(job).toBeDefined();
        expect(job?.type).toBe("send_email");
        expect(job?.payload).toEqual({ to: "me@email.com" });
        expect(job?.status).toBe("pending");
        expect(job?.retries).toBe(0);
        expect(job?.maxRetries).toBe(3);
        expect(job?.runAt).toBeDefined()
        expect(job?.error).toBeUndefined();
    });

    it ("respects a custom maxRetries", async () => {
        const queue = new Queue(TEST_DB, schemaPath);
        const jobId = await queue.enqueue("send_email", {}, { maxRetries: 2});
        const job = queue.getJob(jobId);

        expect(job?.maxRetries).toBe(2);
    });

    it ("returns a unique job ID for each call", async () => {
        const queue = new Queue(TEST_DB, schemaPath);
        const id1 = await queue.enqueue("job", {});
        const id2 = await queue.enqueue("job", {});

        expect(id1).not.toEqual(id2);
    })
})