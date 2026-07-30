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

    it("creates a valid job when enqueued", async() => {
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
});