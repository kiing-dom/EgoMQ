import { describe, it, expect } from "vitest";
import { Queue } from "../src/queue";

const TEST_DB = ":memory:";

describe("enqueue", () => {
    it ("writes a job with the correct shape to the DB", async () => {
        const queue = new Queue(TEST_DB, "./schema.sql");
        const jobId = await queue.enqueue("send_email", { to: "me@email.com"});
        const job = queue.getJob(jobId);

        expect(job).toBeDefined();
        expect(job?.type).toBe("send_email");
        expect(job?.payload).toEqual({ to: "me@email.com" });
        expect(job?.status).toBe("pending");
        expect(job?.retries).toBe(0);
        expect(job?.maxRetries).toBe(3);
        expect(job?.runAt).toBeUndefined();
        expect(job?.error).toBeUndefined();
    });
})