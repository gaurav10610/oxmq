package io.oxmq.model;

/**
 * Options for bulk retrying jobs on a queue.
 * Conforms to BullMQ {@code retryJobs} options.
 */
public class RetryJobsOptions {

    private final JobState state;
    private final int count;
    private final Long timestamp;

    public RetryJobsOptions(JobState state, int count, Long timestamp) {
        this.state = state != null ? state : JobState.FAILED;
        this.count = count > 0 ? count : 1000;
        this.timestamp = timestamp;
    }

    public static RetryJobsOptions defaults() {
        return new RetryJobsOptions(JobState.FAILED, 1000, null);
    }

    public static Builder builder() {
        return new Builder();
    }

    public JobState getState() {
        return state;
    }

    public int getCount() {
        return count;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public static class Builder {
        private JobState state = JobState.FAILED;
        private int count = 1000;
        private Long timestamp = null;

        public Builder state(JobState state) {
            this.state = state;
            return this;
        }

        public Builder count(int count) {
            this.count = count;
            return this;
        }

        public Builder timestamp(Long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public RetryJobsOptions build() {
            return new RetryJobsOptions(state, count, timestamp);
        }
    }
}
