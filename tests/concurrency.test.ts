import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { existsSync, unlinkSync } from "node:fs";
import { Queue } from "../src/queue";

const TEST_DB = "./tests/test-concurrency.db";
const schemaPath = "./schema.sql";
const sendEmail = "send_email";

function cleanupDb() {
  for (const suffix of ["", "-wal", "-shm"]) {
    const path = TEST_DB + suffix;
    if (existsSync(path)) unlinkSync(path);
  }
}

beforeEach(() => {
  cleanupDb();
});

afterEach(() => {
  cleanupDb();
});

describe("concurrent claiming", () => {
  it("never lets two Queue instances claim the same job", async () => {
    const writer = new Queue(TEST_DB, schemaPath);
    const ids: string[] = [];
    for (let i = 0; i < 20; i++) {
      ids.push(await writer.enqueue(sendEmail, { number: i }));
    }

    const worker1 = new Queue(TEST_DB, schemaPath);
    const worker2 = new Queue(TEST_DB, schemaPath);
    worker1.register(sendEmail, async () => {});
    worker2.register(sendEmail, async () => {});

    const claimed1: string[] = [];
    const claimed2: string[] = [];

    try {
      for (let i = 0; i < ids.length; i++) {
        const job1 = await worker1.processNext();
        if (job1) claimed1.push(job1.id);
        const job2 = await worker2.processNext();
        if (job2) claimed2.push(job2.id);
      }

      const allClaimed = [...claimed1, ...claimed2];
      const unique = new Set(allClaimed);

      expect(allClaimed).toHaveLength(unique.size);
      expect(unique.size).toBe(ids.length);
    } finally {
      writer.close();
      worker1.close();
      worker2.close();
    }
  });
});