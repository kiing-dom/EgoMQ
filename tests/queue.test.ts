import { describe, it, expect, vi } from "vitest";
import { Queue } from '../src/queue';

describe("Queue", () => {
    it("calls the registered handler", async () => {
        const queue = new Queue();

        const handler = vi.fn();

        queue.register("hello", handler);
        
        await queue.enqueue({
            type: "hello",
            payload: {
                name: "dom"
            },
        });

        await queue.start();
        
        expect(handler).toHaveBeenCalledOnce();
    })
});