import type { Job } from "./jobs";
import { DEFAULT_MAX_ATTEMPTS } from "./retry";

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
    options?: { maxAttempts?: number },
  ) {
    const maxAttempts = options?.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;

    const job: Job = {
      id: crypto.randomUUID(),
      type,
      payload,
      status: "pending",
      createdAt: new Date(),
      attempts: 0,
      maxAttempts,
    };

    this.jobs.push(job);
  }

  async start() {
    while (this.jobs.length > 0) {
      const job = this.jobs.shift()!;
      const handler = this.handlers.get(job.type);

      if (!handler) {
        console.warn(`No handler registered for the job type ${job.type}`);
        continue;
      }

      try {
        job.status = "running";
        await handler(job.payload);

        job.status = "completed";
      } catch (error) {
        job.attempts += 1;
        job.error = error instanceof Error ? error.message : String(error);

        if (job.attempts < job.maxAttempts) {
          job.status = "pending";
        } else {
          job.status = "failed";
        }
      }
    }
  }

  getJobs(): Job[] {
    return this.jobs;
  }
}
