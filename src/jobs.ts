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

export interface JobRow {
    id: string;
    type: string;
    payload: string;
    status: string;
    created_at: string;
    run_at: string;
    retries: number;
    max_retries: number;
    error: string | null;
}

export interface DeadLetterJob extends Omit<Job, "runAt"> {
    movedAt: Date
}

export interface DeadLetterJobRow {
    id: string;
    type: string;
    payload: string;
    status: string;
    created_at: string;
    retries: number;
    max_retries: number;
    error: string | null;
    moved_at: string;
}

export function rowToJob(row: JobRow): Job {
    return {
        id: row.id,
        type: row.type,
        payload: JSON.parse(row.payload),
        status: row.status as Job["status"],
        createdAt: new Date(row.created_at),
        runAt: new Date(row.run_at),
        retries: row.retries,
        maxRetries: row.max_retries,
        error: row.error ?? undefined,
    }
}

export function rowToDeadLetterJob(row: DeadLetterJobRow): DeadLetterJob {
    return {
        id: row.id,
        type: row.type,
        payload: JSON.parse(row.payload),
        status: row.status as Job["status"],
        createdAt: new Date(row.created_at),
        retries: row.retries,
        maxRetries: row.max_retries,
        error: row.error ?? undefined,
        movedAt: new Date(row.moved_at),
    }
}