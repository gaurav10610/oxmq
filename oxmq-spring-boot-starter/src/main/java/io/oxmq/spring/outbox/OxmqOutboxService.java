package io.oxmq.spring.outbox;

import io.oxmq.model.JobOptions;

/**
 * Service for transactional enqueuing into the outbox.
 */
public interface OxmqOutboxService {

    /**
     * Enqueues a job into the outbox within the current database transaction.
     *
     * @param queueName target queue
     * @param payload payload data
     * @return outbox record id
     */
    String enqueue(String queueName, Object payload);

    /**
     * Enqueues a job with a name into the outbox.
     */
    String enqueue(String queueName, String jobName, Object payload);

    /**
     * Enqueues a job with a name and options into the outbox.
     */
    String enqueue(String queueName, String jobName, Object payload, JobOptions options);

    /**
     * Enqueues a job with a name, virtual group, and options into the outbox.
     */
    String enqueueGroup(String queueName, String jobName, String groupId, Object payload, JobOptions options);
}
