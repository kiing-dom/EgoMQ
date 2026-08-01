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
    const maxRetries = options?.maxRetries ?? DEFAULT_MAX_ATTEMPTS;

    const job: Job = {
      id: crypto.randomUUID(),
      type,
      payload,
      status: "pending",
      createdAt: new Date(),
      retries: 0,
      maxRetries,
    };

    this.jobs.push(job);
    return job;
  }

  async start() {
    while (this.jobs.length > 0) {
      const job = this.jobs.shift()!;
      const handler = this.handlers.get(job.type);

      if (!handler) {
        console.warn(`No handler registered for the job type ${job.type}`);
        continue;
      }

      await processJob(job, handler);

      if (job.status === "pending") {
        this.jobs.push(job);
      }
    }
  }

  getJobs(): Job[] {
    return this.jobs;
  }
}
