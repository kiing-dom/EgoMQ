import { Job } from "../jobs";

export async function processJob(job: Job, handler: (payload: unknown) => Promise<void>) {
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