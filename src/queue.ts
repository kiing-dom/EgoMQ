import type { Job } from "./jobs";
import { DEFAULT_MAX_ATTEMPTS } from "./retry";
import { processJob } from "./utils/jobUtils";

type JobHandler = (payload: any) => Promise<void> | void;

export class Queue {
  private readonly handlers: Map<string, JobHandler> = new Map();
  private readonly jobs: Job[] = [];

  register(type: string, handler: JobHandler) {
    this.handlers.set(type, handler);
  }

  async enqueue(
    type: string,
    payload: unknown,
    options?: { maxRetries?: number },
  ) : Promise<Job> {
    const now = new Date();
    const maxRetries = options?.maxRetries ?? DEFAULT_MAX_ATTEMPTS;

    const job: Job = {
      id: crypto.randomUUID(),
      type,
      payload,
      status: "pending",
      createdAt: now,
      runAt: now,
      retries: 0,
      maxRetries,
    };

    this.jobs.push(job);
    return job;
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
}
