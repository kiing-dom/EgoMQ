import { Job } from "../src/jobs";

export function makeJobMock(overrides?: Partial<Job>): Job {
    return {
        id: "job-abc123",
        type: "lock_in",
        payload: {},
        status: "pending",
        createdAt: new Date(),
        attempts: 0,
        maxAttempts: 3,
        ...overrides,
    };
};