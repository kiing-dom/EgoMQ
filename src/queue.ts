import type { Job } from "./jobs";

type JobHandler = (payload: any) => Promise<void> | void;

export class Queue {
  private readonly handlers: Map<string, JobHandler> = new Map();
  private readonly jobs: Job[] = [];

  register(type: string, handler: JobHandler) {
    this.handlers.set(type, handler);
  }

  async enqueue(type: string, payload: unknown) {
    const job: Job = {
      id: crypto.randomUUID(),
      type,
      payload,
      status: "pending",
      createdAt: new Date(),
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
        job.status = "failed";
        job.error = error instanceof Error ? error.message : String(error);
      }
    }
  }

  getJobs(): Job[] {
    return this.jobs;
  }
}
