package io.oxmq.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Hierarchical workflow node representing a job and its recursive child dependency jobs in a DAG flow tree.
 * Mirrors BullMQ's JobNode.
 */
public class JobNode {

    private final Job<?> job;
    private final List<JobNode> children;

    public JobNode(Job<?> job) {
        this(job, new ArrayList<>());
    }

    public JobNode(Job<?> job, List<JobNode> children) {
        this.job = job;
        this.children = children != null ? children : new ArrayList<>();
    }

    public Job<?> getJob() {
        return job;
    }

    public List<JobNode> children() {
        return children;
    }

    public List<JobNode> getChildren() {
        return children;
    }

    public void addChild(JobNode child) {
        if (child != null) {
            this.children.add(child);
        }
    }

    @Override
    public String toString() {
        return "JobNode{" +
                "job=" + (job != null ? job.getId() : null) +
                ", childrenCount=" + children.size() +
                '}';
    }
}
