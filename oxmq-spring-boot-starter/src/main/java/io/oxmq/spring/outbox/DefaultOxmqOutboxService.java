package io.oxmq.spring.outbox;

import io.oxmq.model.JobOptions;
import io.oxmq.serializer.JobSerializer;

import java.util.Objects;

/**
 * Default implementation of {@link OxmqOutboxService} serializing payloads and persisting records.
 */
public class DefaultOxmqOutboxService implements OxmqOutboxService {

    private final OxmqOutboxRepository repository;
    private final JobSerializer serializer;
    private final String defaultOptionsJson;

    public DefaultOxmqOutboxService(OxmqOutboxRepository repository, JobSerializer serializer) {
        this.repository = Objects.requireNonNull(repository, "repository must not be null");
        this.serializer = Objects.requireNonNull(serializer, "serializer must not be null");
        this.defaultOptionsJson = this.serializer.serialize(JobOptions.defaults());
    }

    @Override
    public String enqueue(String queueName, Object payload) {
        return enqueue(queueName, "default", payload, null);
    }

    @Override
    public String enqueue(String queueName, String jobName, Object payload) {
        return enqueue(queueName, jobName, payload, null);
    }

    @Override
    public String enqueue(String queueName, String jobName, Object payload, JobOptions options) {
        return enqueueGroup(queueName, jobName, null, payload, options);
    }

    @Override
    public String enqueueGroup(String queueName, String jobName, String groupId, Object payload, JobOptions options) {
        Objects.requireNonNull(queueName, "queueName must not be null");
        String effectiveGroup = groupId;
        String optionsJson;
        if (options == null && effectiveGroup == null) {
            optionsJson = defaultOptionsJson;
        } else {
            JobOptions opts = options != null ? options : JobOptions.defaults();
            if (effectiveGroup != null && !effectiveGroup.isBlank()) {
                opts.group(effectiveGroup);
            } else if (opts.getGroup() != null && opts.getGroup().getId() != null) {
                effectiveGroup = opts.getGroup().getId();
            }
            optionsJson = serializer.serialize(opts);
        }

        String payloadJson = payload != null ? serializer.serialize(payload) : "{}";

        OxmqOutboxRecord record = new OxmqOutboxRecord(queueName, jobName, payloadJson, optionsJson, effectiveGroup);
        repository.save(record);
        return record.getId();
    }
}
