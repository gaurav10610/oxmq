package io.oxmq;

import io.oxmq.model.Job;
import java.util.List;

/**
 * Functional interface for processing a batch of OxMQ background jobs atomically.
 * Designed for high-throughput database ingestion (Elasticsearch, PostgreSQL batch insert, ClickHouse, S3).
 *
 * @param <T> Payload data type
 * @param <R> Return value type
 */
@FunctionalInterface
public interface BatchJobProcessor<T, R> {

    /**
     * Executes batch processing logic over multiple jobs.
     *
     * @param jobs List of jobs popped in this batch (up to configured batchSize)
     * @return Execution return value
     * @throws Exception Any exception triggering batch failure or retries
     */
    R process(List<Job<T>> jobs) throws Exception;
}
