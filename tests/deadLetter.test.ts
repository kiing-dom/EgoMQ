import { describe, it, expect, vi } from 'vitest';
import { Queue } from '../src/queue';

const TEST_DB = ":memory:";
const schemaPath = "./schema.sql";
const sendEmail = "send_email";

describe("making sure the dead letter queue helpers work", () => {
    it ("gets the dead letter queue job count correctly", async () => {
        vi.useFakeTimers();

        const queue = new Queue(TEST_DB, schemaPath);
        const handler = vi.fn().mockRejectedValue(new Error("Ls"));

        queue.register(sendEmail, handler);

        await queue.enqueue(sendEmail, {});
        await queue.enqueue(sendEmail, {});

        const startPromise = queue.start();

        vi.runAllTimersAsync();
        await startPromise;

        expect(queue.getDeadLetterCount()).toBe(2);
        expect(queue.getDeadLetterQueueJobs()).toBeDefined()

        vi.useRealTimers()
    });

    it ("purgeDeadLetterQueue() removes all rows from dead_letter_jobs", async () => {
        vi.useFakeTimers();

        const queue = new Queue(TEST_DB, schemaPath);
        const handler = vi.fn().mockRejectedValue(new Error("fail!"));

        queue.register(sendEmail, handler);

        await queue.enqueue(sendEmail, {});
        await queue.enqueue(sendEmail, {});

        const startPromise = queue.start();

        vi.runAllTimersAsync();
        await startPromise;

        expect(queue.getDeadLetterCount()).toBe(2);

        queue.purgeDeadLetterQueue();

        expect(queue.getDeadLetterCount()).toBe(0);
    });

    it ("purgeDeadLetterJob(jobId) removes the row of matching id from dead_letter_jobs", async () => {
        vi.useFakeTimers();

        const queue = new Queue(TEST_DB, schemaPath);
        const handler = vi.fn().mockRejectedValue(new Error("fail!"));

        queue.register(sendEmail, handler);

        const id1 = await queue.enqueue(sendEmail, {});
        await queue.enqueue(sendEmail, {});

        const startPromise = queue.start();

        vi.runAllTimersAsync();
        await startPromise;

        expect(queue.getDeadLetterCount()).toBe(2);

        queue.purgeDeadLetterJob(id1);

        expect(queue.getDeadLetterCount()).toBe(1);
    });
})