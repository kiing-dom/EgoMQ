import { Job } from "../src/jobs";

export function makeJobMock(overrides?: Partial<Job>): Job {
    return {
        id: "job-abc123",
        type: "lock_in",
        payload: {},
        status: "pending",
        createdAt: new Date(),
        retries: 0,
        maxRetries: 3,
        ...overrides,
    };
};