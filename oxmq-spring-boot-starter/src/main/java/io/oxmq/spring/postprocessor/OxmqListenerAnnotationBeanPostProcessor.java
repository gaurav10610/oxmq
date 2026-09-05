package io.oxmq.spring.postprocessor;

import io.lettuce.core.RedisClient;
import io.oxmq.OxmqWorker;
import io.oxmq.Worker;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.lua.LuaScriptManager;
import io.oxmq.metrics.OxmqMetrics;
import io.oxmq.model.Job;
import io.oxmq.serializer.JobSerializer;
import io.oxmq.spring.OxmqProperties;
import io.oxmq.spring.annotation.OxmqListener;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;

/**
 * Discovers beans with {@link OxmqListener} annotations and manages their worker lifecycles.
 */
public class OxmqListenerAnnotationBeanPostProcessor implements BeanPostProcessor, SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(OxmqListenerAnnotationBeanPostProcessor.class);

    private final RedisClient redisClient;
    private final LuaScriptManager scriptManager;
    private final JobSerializer serializer;
    private final OxmqMetrics metrics;
    private final OxmqProperties properties;
    private final List<Worker<?>> registeredWorkers = new ArrayList<>();
    private volatile boolean running = false;

    public OxmqListenerAnnotationBeanPostProcessor(RedisClient redisClient, LuaScriptManager scriptManager,
                                                   JobSerializer serializer, OxmqMetrics metrics,
                                                   OxmqProperties properties) {
        this.redisClient = redisClient;
        this.scriptManager = scriptManager;
        this.serializer = serializer;
        this.metrics = metrics;
        this.properties = properties;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> targetClass = bean.getClass();
        var annotatedMethods = MethodIntrospector.selectMethods(targetClass,
                (MethodIntrospector.MetadataLookup<OxmqListener>) method ->
                        AnnotatedElementUtils.findMergedAnnotation(method, OxmqListener.class));

        for (var entry : annotatedMethods.entrySet()) {
            Method method = entry.getKey();
            OxmqListener listener = entry.getValue();
            processListenerMethod(bean, method, listener);
        }

        return bean;
    }

    private void processListenerMethod(Object bean, Method method, OxmqListener listener) {
        String queueName = listener.queue();
        int concurrency = listener.concurrency() > 0 ? listener.concurrency() : properties.getDefaultConcurrency();
        boolean useVirtualThreads = listener.virtualThreads();
        long lockDurationMs = listener.lockDurationMs();
        long pollIntervalMs = listener.pollIntervalMs();

        method.setAccessible(true);
        Class<?>[] paramTypes = method.getParameterTypes();

        var builder = OxmqWorker.builder()
                .queueName(queueName)
                .connectionManager(new RedisConnectionManager(redisClient))
                .scriptManager(scriptManager)
                .serializer(serializer)
                .metrics(metrics)
                .concurrency(concurrency)
                .useVirtualThreads(useVirtualThreads)
                .lockDurationMs(lockDurationMs)
                .pollIntervalMs(pollIntervalMs);

        if (listener.rateLimitMax() > 0 && listener.rateLimitDurationMs() > 0) {
            builder.rateLimit(listener.rateLimitMax(), Duration.ofMillis(listener.rateLimitDurationMs()));
        }

        builder.processor(job -> {
            if (paramTypes.length == 0) {
                return method.invoke(bean);
            } else if (paramTypes.length == 1 && Job.class.isAssignableFrom(paramTypes[0])) {
                return method.invoke(bean, job);
            } else if (paramTypes.length == 1) {
                Object data = job.getData();
                if (data instanceof String str && !String.class.isAssignableFrom(paramTypes[0])) {
                    data = serializer.deserialize(str, paramTypes[0]);
                }
                return method.invoke(bean, data);
            } else {
                throw new IllegalArgumentException("@OxmqListener method must have 0 or 1 parameter: " + method);
            }
        });

        Worker<?> worker = builder.build();
        registeredWorkers.add(worker);
        log.info("Registered @OxmqListener worker for queue '{}' -> {}.{}()", queueName, bean.getClass().getSimpleName(), method.getName());
    }

    @Override
    public void start() {
        if (!running) {
            log.info("Starting {} OxMQ listener worker(s)...", registeredWorkers.size());
            for (Worker<?> worker : registeredWorkers) {
                worker.start();
            }
            running = true;
        }
    }

    @Override
    public void stop() {
        if (running) {
            log.info("Stopping {} OxMQ listener worker(s)...", registeredWorkers.size());
            for (Worker<?> worker : registeredWorkers) {
                worker.close();
            }
            running = false;
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    public List<Worker<?>> getRegisteredWorkers() {
        return registeredWorkers;
    }
}
