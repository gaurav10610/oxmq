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
import io.oxmq.spring.annotation.OxListener;
import io.oxmq.spring.annotation.OxmqListener;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
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
 * Discovers beans with {@link OxmqListener} or {@link OxListener} annotations and manages their worker lifecycles.
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
                (MethodIntrospector.MetadataLookup<OxmqListener>) method -> {
                    OxmqListener oxmqListener = AnnotatedElementUtils.findMergedAnnotation(method, OxmqListener.class);
                    if (oxmqListener != null) return oxmqListener;
                    OxListener oxListener = AnnotatedElementUtils.findMergedAnnotation(method, OxListener.class);
                    if (oxListener != null) {
                        return new OxmqListener() {
                            public Class<? extends java.lang.annotation.Annotation> annotationType() { return OxmqListener.class; }
                            public String queue() { return oxListener.queue(); }
                            public int concurrency() { return oxListener.concurrency(); }
                            public boolean virtualThreads() { return oxListener.virtualThreads() && oxListener.useVirtualThreads(); }
                            public long lockDurationMs() { return oxListener.lockDurationMs(); }
                            public long pollIntervalMs() { return oxListener.pollIntervalMs(); }
                            public int rateLimitMax() { return oxListener.rateLimitMax(); }
                            public long rateLimitDurationMs() { return oxListener.rateLimitDurationMs(); }
                        };
                    }
                    return null;
                });

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
        Class<?> payloadClass = null;

        if (paramTypes.length == 1) {
            if (Job.class.isAssignableFrom(paramTypes[0])) {
                Type genericType = method.getGenericParameterTypes()[0];
                if (genericType instanceof ParameterizedType pt) {
                    Type[] typeArgs = pt.getActualTypeArguments();
                    if (typeArgs.length > 0 && typeArgs[0] instanceof Class<?> clazz) {
                        payloadClass = clazz;
                    }
                }
            } else {
                payloadClass = paramTypes[0];
            }
        }

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

        if (payloadClass != null) {
            builder.payloadClass((Class) payloadClass);
        }

        if (listener.rateLimitMax() > 0 && listener.rateLimitDurationMs() > 0) {
            builder.rateLimit(listener.rateLimitMax(), Duration.ofMillis(listener.rateLimitDurationMs()));
        }

        builder.processor(job -> {
            try {
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
            } catch (java.lang.reflect.InvocationTargetException ite) {
                Throwable target = ite.getTargetException();
                if (target instanceof Exception ex) throw ex;
                if (target instanceof Error err) throw err;
                throw ite;
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
