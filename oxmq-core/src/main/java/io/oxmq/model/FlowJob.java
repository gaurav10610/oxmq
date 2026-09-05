package io.oxmq.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Representation of a node in a parent-child DAG task tree.
 */
public class FlowJob<T> {

    private final String queueName;
    private final String name;
    private final T data;
    private final JobOptions opts;
    private final List<FlowJob<?>> children = new ArrayList<>();

    private FlowJob(String queueName, String name, T data, JobOptions opts) {
        this.queueName = queueName;
        this.name = name;
        this.data = data;
        this.opts = opts != null ? opts : new JobOptions();
    }

    public static <T> FlowJob<T> of(String queueName, String name, T data) {
        return new FlowJob<>(queueName, name, data, new JobOptions());
    }

    public static <T> FlowJob<T> of(String queueName, String name, T data, JobOptions opts) {
        return new FlowJob<>(queueName, name, data, opts);
    }

    public static FlowJob<Void> of(String queueName, String name) {
        return new FlowJob<>(queueName, name, null, new JobOptions());
    }

    public FlowJob<T> addChild(FlowJob<?> child) {
        if (child != null) {
            children.add(child);
        }
        return this;
    }

    public FlowJob<T> addChildren(List<FlowJob<?>> childList) {
        if (childList != null) {
            children.addAll(childList);
        }
        return this;
    }

    public String getQueueName() {
        return queueName;
    }

    public String getName() {
        return name;
    }

    public T getData() {
        return data;
    }

    public JobOptions getOpts() {
        return opts;
    }

    public List<FlowJob<?>> getChildren() {
        return Collections.unmodifiableList(children);
    }
}
