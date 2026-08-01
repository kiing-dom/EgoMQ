interface RetryPolicy {
    maxAttempts: number;
    isRetryable?: (err: unknown) => boolean;
}

const RETRY_POLICY: Record<string, RetryPolicy> = {
    sendEmail: { maxAttempts: 5 },
    chargePayment: { maxAttempts: 3 },
    generateReport: { maxAttempts: 2 },
    webhookDelivery: { maxAttempts: 8 },
};

const DEFAULT_POLICY: RetryPolicy = { maxAttempts: 3 };