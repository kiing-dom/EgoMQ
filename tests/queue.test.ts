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

        await queue.start();
        
        expect(handler).toHaveBeenCalledOnce();
    });

    it("creates a valid job when enqueued", async () => {
        const queue = new Queue();

        const handler = vi.fn();

        queue.register("hi", handler);
        await queue.enqueue("hi", {
            name: "dom",
        });

        const job: Job = queue.getJobs()[0];
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
        await queue.enqueue("get_balance", {
            balance: 9.50,
        });

        const job: Job = queue.getJobs()[0];
        expect(job.status).toBe("pending");
        await queue.start();
        expect(job.status).toBe("completed");
    });

    it ("requeues failed so 'start()' eventually processes it again", async () => {
        const queue = new Queue();

        const handler = vi.fn()
            .mockRejectedValueOnce(new Error("idk"))
            .mockResolvedValueOnce(undefined);

        queue.register("lock_in", handler);
        const job = await queue.enqueue("lock_in", {to: "dom@email.com"});

        await queue.start();

        expect(job.status).toBe("completed");
        expect(job.retries).toBe(1);
        expect(handler).toHaveBeenCalledTimes(2);
    });

    it ("stops trying after maxRetries is exhausted and 'start()' terminates", async () => {
        const queue = new Queue();

        const handler = vi.fn().mockRejectedValue(new Error("it's broke"));
        queue.register("tough_times", handler);

        const job = await queue.enqueue("tough_times", {}, { maxRetries: 4});
        
        await queue.start();

        expect(job.status).toBe("failed");
        expect(job.retries).toBe(4);
        expect(handler).toHaveBeenCalledTimes(4);
    })
});