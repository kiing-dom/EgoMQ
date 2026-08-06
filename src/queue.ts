import type { DeadLetterJob, DeadLetterJobRow, Job, JobRow } from "./jobs";
import { rowToJob, rowToDeadLetterJob } from "./jobs";

import { DEFAULT_MAX_ATTEMPTS } from "./retry";
import { processJob } from "./utils/jobUtils";

import { Database } from "bun:sqlite";
import { readFileSync } from "node:fs";

type JobHandler = (payload: any) => Promise<void> | void;

export class Queue {
  private readonly handlers: Map<string, JobHandler> = new Map();
  private db: Database;

  constructor(dbPath: string, schemaPath: string) {
    this.db = new Database(dbPath);
    this.db.run(readFileSync(schemaPath, "utf8"));

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

  async enqueue(
    type: string,
    payload: unknown,
    options?: { maxRetries: number },
  ) {
    const now = new Date();
    const job: Job = {
      id: crypto.randomUUID(),
      type,
      payload,
      status: "pending",
      createdAt: now,
      retries: 0,
      maxRetries: options?.maxRetries ?? 3,
      runAt: now,
    };

    this.db
      .query(
        `INSERT INTO jobs (id, type, payload, status, created_at, retries, max_retries, run_at)
          VALUES ($id, $type, $payload, $status, $createdAt, $retries, $maxRetries, $runAt)`,
      )
      .run({
        $id: job.id,
        $type: job.type,
        $payload: JSON.stringify(job.payload),
        $status: job.status,
        $createdAt: job.createdAt.toISOString(),
        $retries: job.retries,
        $maxRetries: job.maxRetries,
        $runAt: job.runAt.toISOString(),
      });

    return job.id;
  }

  async processNext(): Promise<Job | undefined> {
    const now = new Date();

    const dueRow = this.db
      .query(
        `SELECT * FROM jobs WHERE status = 'pending' AND run_at <= $now
          ORDER BY created_at ASC LIMIT 1`,
      )
      .get({ $now: now.toISOString() }) as any;

    if (!dueRow) return undefined;

    const job = rowToJob(dueRow);
    const handler = this.handlers.get(job.type);

    if (!handler) {
      console.warn(`No handler registered for job type: ${job.type}`);
      // NOTE: in the in-memory version, we removed it permanently
      // but it might be better to hold the unregistered job, will be status "pending"
      // until the job is either removed manually or actually registered
      return job;
    }

    await processJob(job, handler);
    this.persistJobState(job);

    return job;
  }

  async start(): Promise<void> {
    while (await this.hasActiveJobs()) {
      const processed = await this.processNext();
      if (processed) continue;

      const nextRunAt = this.getNextPendingRunAt();
      if (!nextRunAt) return;

      const waitMs = Math.max(0, nextRunAt.getTime() - Date.now());
      await new Promise((resolve) => setTimeout(resolve, waitMs));
    }
  }

  private async hasActiveJobs(): Promise<boolean> {
    const row = this.db
      .query(
        `SELECT COUNT(*) as count FROM jobs WHERE status IN ('pending', 'running')`,
      )
      .get() as any;

    return row.count > 0;
  }

  getJob(jobId: string): Job | undefined {
    const row = this.db
      .query(`SELECT * FROM jobs WHERE id = $id`)
      .get({ $id: jobId }) as JobRow | null;
    return row ? rowToJob(row) : undefined;
  }

  getDeadLetterQueueJobs(): DeadLetterJob[] | undefined {
    const rows = this.db
    .query(`SELECT * FROM dead_letter_jobs`)
    .all() as DeadLetterJobRow[];

    return rows.map((row) => rowToDeadLetterJob(row));
  }

  getDeadLetterCount(): number {
    const query = this.db
      .query(`SELECT COUNT(*) AS count FROM dead_letter_jobs`)
      .get() as { count: number }

      return query?.count ?? 0;
  }

  private persistJobState(job: Job) {
    if (job.status === "failed") {
      this.moveToDeadLetter(job);
      return;
    }

    this.db
      .query(
        `UPDATE jobs
        SET STATUS = $status, retries = $retries, run_at = $runAt, error = $error
        where id = $id`,
      )
      .run({
        $status: job.status,
        $retries: job.retries,
        $runAt: job.runAt.toISOString(),
        $error: job.error ?? null,
        $id: job.id,
      });
  }

  private getNextPendingRunAt(): Date | undefined {
    const nextRow = this.db
      .query(
        `SELECT run_at FROM jobs WHERE status = 'pending' ORDER BY run_at ASC LIMIT 1`
      )
      .get() as { run_at: number } | undefined;

      if (!nextRow) return undefined;

    return new Date(nextRow.run_at)
  }

  private moveToDeadLetter(job: Job) {
    const moveTx = this.db.transaction((job: Job) => {
      this.db.query(`DELETE FROM jobs WHERE id = $id`).run({ $id: job.id });

      this.db
        .query(
          `INSERT INTO dead_letter_jobs
        (id, type, payload, status, created_at, retries, max_retries, error, moved_at)
        VALUES ($id, $type, $payload, $status, $createdAt, $retries, $maxRetries, $error, $movedAt)`,
        )
        .run({
          $id: job.id,
          $type: job.type,
          $payload: JSON.stringify(job.payload),
          $status: job.status,
          $createdAt: job.createdAt.toISOString(),
          $retries: job.retries,
          $maxRetries: job.maxRetries,
          $error: job.error ?? null,
          $movedAt: new Date().toISOString(),
        });
    });

    moveTx(job); // if either query fails/throws, both are rolled back
  }

  retryDeadLetter(jobId: string): void {
    const row = this.db
      .query(`SELECT * FROM dead_letter_jobs WHERE id = $id`)
      .get({ $id: jobId }) as DeadLetterJobRow | null;

      if (!row) {
        throw new Error(`No dead-lettered job found with id "${jobId}`);
      }

      const now = new Date().toISOString();

      this.db.transaction(() => {
        this.db
          .query(`DELETE FROM dead_letter_jobs WHERE id = $id`)
          .run({ $id: jobId });

        this.db
          .query(
            `INSERT INTO jobs (id, type, payload, status, created_at, retries, max_retries, run_at)
            VALUES ($id, $type, $payload, 'pending', $created_at, 0, $max_retries, $run_at)`
          )
          .run({
            $id: row.id,
            $type: row.type,
            $payload: row.payload,
            $created_at: row.created_at,
            $max_retries: row.max_retries,
            $run_at: now,
          });
      })();
  }

  purgeDeadLetterQueue(): void {
    this.db.query(`DELETE FROM dead_letter_jobs`).run();
  }

  purgeDeadLetterJob(jobId: string): void {
    const result = this.db
      .query(`DELETE FROM dead_letter_jobs WHERE id = $id`)
      .run({ $id: jobId });

    if (result.changes === 0) {
      throw new Error(`No dead-lettered job found with id "${jobId}"`);
    }
  }

  // test only helper
  _setRunAtForTest(jobId: string, runAt: Date) {
    this.db
      .query(`UPDATE jobs SET run_at = $runAt WHERE id = $id`)
      .run({ $runAt: runAt.toISOString(), $id: jobId });
  }
}