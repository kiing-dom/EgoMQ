import type { Job, JobRow } from "./jobs";
import { rowToJob } from "./jobs";

import { DEFAULT_MAX_ATTEMPTS } from "./retry";
import { processJob } from "./utils/jobUtils";

import { Database } from "bun:sqlite";
import { readFileSync } from "node:fs"

type JobHandler = (payload: any) => Promise<void> | void;

export class Queue {
  private readonly handlers: Map<string, JobHandler> = new Map();
  private db: Database;

  private readonly jobs: Job[] = []; // removing when db stuff is sorted
  private readonly deadLetterQueue: Job[] = [];

  constructor(dbPath: string, schemaPath: string) {
    this.db = new Database(dbPath);
    this.db.run(readFileSync(schemaPath, 'utf8'));

    /* crash recovery - anything left running from a previous process
     didnt actually finish so it goes back in the queue
     */
    this.db
      .query(`UPDATE jobs SET status = 'pending' WHERE status = 'running'`)
      .run();
  }

  register(type: string, handler: JobHandler) {
    this.handlers.set(type, handler);
  }

  async enqueue(type: string, payload: unknown, maxRetries = 3) {
    const job: Job = {
      id: crypto.randomUUID(),
      type,
      payload,
      status: "pending",
      createdAt: new Date(),
      retries: 0,
      maxRetries,
    };

    this.db
      .query(
        `INSERT INTO jobs (id, type, payload, status, created_at, retries, max_retries)
          VALUES ($id, $type, $payload, $status, $createdAt, $retries, $maxRetries)`
      )
      .run({
        $id: job.id,
        $type: job.type,
        $payload: JSON.stringify(job.payload),
        $status: job.status,
        $createdAt: job.createdAt.toISOString(),
        $retries: job.retries,
        $maxRetries: job.maxRetries,
      });

      return job.id;
  }

  async processNext(): Promise<Job | undefined> {
    const now = new Date();
    const index = this.jobs.findIndex(
      (j) => j.status === "pending" && j.runAt <= now
    );

    if (index === -1) {
      if (this.jobs.length === 0) return undefined;

      const nextRunAt = this.jobs.reduce(
        (earliest, j) => (j.runAt < earliest ? j.runAt : earliest),
        this.jobs[0].runAt
      );
      const waitMs = Math.max(0, nextRunAt.getTime() - now.getTime());
      await new Promise((resolve) => setTimeout(resolve, waitMs));
    }

    const [job] = this.jobs.splice(index, 1);
    const handler = this.handlers.get(job.type);

    if (!handler) {
      console.warn(`No handler registered for the job type: ${job.type}`);
      return job;
    }

    await processJob(job, handler);

    if (job.status === "pending") {
      this.jobs.push(job);
    } else if (job.status === "failed") {
      this.deadLetterQueue.push(job);
    }

    return job
  }

  async start() {
    while (this.jobs.length > 0) {
      await this.processNext();
    }
  }

  getJobs(): Job[] {
    return this.jobs;
  }

  getJob(jobId: string): Job | undefined {
    const row = this.db
      .query(`SELECT * FROM jobs WHERE id = $id`)
      .get({ $id: jobId }) as JobRow | null;
    return row ? rowToJob(row) : undefined;
  }

  getDeadLetterQueueJobs(): Job[] {
    return this.deadLetterQueue;
  }

  retryDeadLetter(jobId: string): void {
    const index = this.deadLetterQueue.findIndex((j) => j.id === jobId);

    if (index === -1) {
      throw new Error(`No dead-lettered job found with id "${jobId}"`);
    }

    const [job] = this.deadLetterQueue.splice(index, 1);

    job.status = "pending";
    job.retries = 0;
    job.runAt = new Date();
    job.error = undefined;

    this.jobs.push(job);
  }

  purgeDeadLetterQueue(): void {
    this.deadLetterQueue.length = 0;
  }

  purgeDeadLetterJob(jobId: string): void {
    const index = this.deadLetterQueue.findIndex((j) => j.id === jobId);

    if (index === -1) {
      throw new Error(`No dead-lettered job found with id "${jobId}"`);
    }
    
    this.deadLetterQueue.splice(index, 1);
  }
}
