import { describe, it, expect, vi } from "vitest";
import { Queue } from '../src/queue';
import { Job } from '../src/jobs';

const UUID_V4_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i

describe("Queue", () => {
    it("calls the registered handler", async () => {
        const queue = new Queue();

        const handler = vi.fn();

        queue.register("hello", handler);
        
        await queue.enqueue("hello", {
            name: "dom",
        });

        await queue.processNext();
        
        expect(handler).toHaveBeenCalledOnce();
    });

    it("creates a valid job when enqueued", async () => {
        const queue = new Queue();

        const handler = vi.fn();

        queue.register("hi", handler);

        const job: Job = await queue.enqueue("hi", {
            name: "dom",
        });

        expect(job).toMatchObject({
            id: expect.stringMatching(UUID_V4_REGEX),
            type: 'hi',
            payload: { name: 'dom'},
            status: "pending",
            createdAt: expect.any(Date),
        });
    });

    it("sets job as 'completed' when successful", async () => {
        const queue = new Queue();

        const handler = vi.fn();

        queue.register("get_balance", handler);
        const job: Job = await queue.enqueue("get_balance", {
            balance: 9.50,
        });

        expect(job.status).toBe("pending");
        await queue.processNext();
        expect(job.status).toBe("completed");
    });

    it ("requeues failed so 'start()' eventually processes it again", async () => {
        vi.useFakeTimers();
        const queue = new Queue();

        const handler = vi.fn()
            .mockRejectedValueOnce(new Error("idk"))
            .mockResolvedValueOnce(undefined);

        queue.register("lock_in", handler);
        const job = await queue.enqueue("lock_in", {to: "dom@email.com"});

        const startPromise = queue.start();

        await vi.runAllTimersAsync();
        await startPromise;

        expect(job.status).toBe("completed");
        expect(job.retries).toBe(1);
        expect(handler).toHaveBeenCalledTimes(2);

        vi.useRealTimers();
    });

    it ("stops trying after maxRetries is exhausted and 'start()' terminates", async () => {
        vi.useFakeTimers();

        const queue = new Queue();

        const handler = vi.fn().mockRejectedValue(new Error("it's broke"));
        queue.register("tough_times", handler);

        const job = await queue.enqueue("tough_times", {}, { maxRetries: 2});
        
        const startPromise = queue.start();

        await vi.runAllTimersAsync();
        await startPromise;

        expect(job.status).toBe("failed");
        expect(job.retries).toBe(2);
        expect(handler).toHaveBeenCalledTimes(2);

        vi.useRealTimers();
    });

    it ("adds failed job to dead letter queue after maxRetries is exhausted without success", async () => {
        vi.useFakeTimers();

        const queue = new Queue();

        const handler = vi.fn().mockRejectedValue(new Error("it's really broke"));
        queue.register("never_last", handler);

        await queue.enqueue("never_last", {});

        const startPromise = queue.start();

        await vi.runAllTimersAsync();
        await startPromise;

        expect(queue.getDeadLetterQueueJobs()).toHaveLength(1);

        vi.useRealTimers();
    })

    it ("processNext() waits for backoff before returning the retried job", async () => {
        vi.useFakeTimers();

        const queue = new Queue();
        const handler = vi.fn()
            .mockRejectedValueOnce(new Error("failed"))
            .mockResolvedValueOnce(undefined);
        
        queue.register("some_job_event", handler);

        await queue.enqueue("some_job_event", {});
        await queue.processNext(); // should fail if i'm not stupid

        const resultPromise = queue.processNext();
        await vi.advanceTimersByTimeAsync(5000);
        const result = await resultPromise;

        expect(result?.status).toBe("completed");
        vi.useRealTimers();
    })
});