import { Job } from "../jobs";

export async function processJob(job: Job, handler: (payload: unknown) => void | Promise<void>) {
    try {
        job.status = "running";
        await handler(job.payload);
        job.status = "completed";
    } catch (error) {
        job.error = error instanceof Error ? error.message : String(error);
        
        if (job.retries < job.maxRetries) {
            job.retries += 1;
            job.status = "pending";
            
            const backoffMs = Math.min(1000 * 2 * job.retries, 30_000);
            job.runAt = new Date(Date.now() + backoffMs);
        } else {
            job.status = "failed";
        }
    }
}