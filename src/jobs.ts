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
    runAt?: Date;
    retries: number;
    maxRetries: number;
    error?: string;
}

export interface JobRow {
    id: string;
    type: string;
    payload: string;
    status: string;
    created_at: number;
    run_at: number | null;
    retries: number;
    max_retries: number;
    error: string | null;
}

export function rowToJob(row: JobRow): Job {
    return {
        id: row.id,
        type: row.type,
        payload: JSON.parse(row.payload),
        status: row.status as Job["status"],
        createdAt: new Date(row.created_at),
        runAt: row.run_at !== null ? new Date(row.run_at) : undefined,
        retries: row.retries,
        maxRetries: row.max_retries,
        error: row.error ?? undefined,
    }
}