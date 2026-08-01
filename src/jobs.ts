export type JobStatus =
| "pending"
| "running"
| "completed"
| "failed";

export interface Job {
    id: string;
    type: string;
    payload: unknown;
    status: JobStatus;
    createdAt: Date;
    runAt: Date;
    retries: number;
    maxRetries: number;
    error?: string;
}