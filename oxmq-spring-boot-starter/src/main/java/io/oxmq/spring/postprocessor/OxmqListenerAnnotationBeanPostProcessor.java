package io.oxmq.spring.postprocessor;

import io.lettuce.core.RedisClient;
import io.oxmq.OxmqBatchWorker;
import io.oxmq.OxmqWorker;
import io.oxmq.Worker;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.lua.LuaScriptManager;
import io.oxmq.metrics.OxmqMetrics;
import io.oxmq.model.Job;
import io.oxmq.serializer.JacksonJobSerializer;
import io.oxmq.serializer.JobSerializer;
import io.oxmq.spring.OxmqProperties;
import io.oxmq.spring.annotation.Header;
import io.oxmq.spring.annotation.OxListener;
import io.oxmq.spring.annotation.OxmqBatchListener;
import io.oxmq.spring.annotation.OxmqHandler;
import io.oxmq.spring.annotation.OxmqListener;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.util.StringValueResolver;

/**
 * Discovers Spring beans with {@link OxmqListener}, {@link OxmqBatchListener}, or class-level
 * {@link OxmqListener} with {@link OxmqHandler} methods and manages their worker lifecycles.
 */
public class OxmqListenerAnnotationBeanPostProcessor implements
        BeanPostProcessor,
        ApplicationContextAware,
        SmartLifecycle,
        EmbeddedValueResolverAware {

    private static final Logger log = LoggerFactory.getLogger(OxmqListenerAnnotationBeanPostProcessor.class);

    private ApplicationContext applicationContext;
    private StringValueResolver valueResolver;

    private RedisClient redisClient;
    private RedisConnectionManager connectionManager;
    private LuaScriptManager scriptManager;
    private JobSerializer serializer;
    private OxmqMetrics metrics;
    private OxmqProperties properties;

    private final List<Worker<?>> registeredWorkers = new ArrayList<>();
    private volatile boolean running = false;

    public OxmqListenerAnnotationBeanPostProcessor() {
    }

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
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public void setEmbeddedValueResolver(StringValueResolver resolver) {
        this.valueResolver = resolver;
    }

    private String resolve(String value) {
        if (value == null || value.isBlank()) return value;
        return valueResolver != null ? valueResolver.resolveStringValue(value) : value;
    }

    private String resolveEffectivePrefix(String annotationPrefix) {
        String resolved = resolve(annotationPrefix);
        if (resolved != null && !resolved.isBlank()) {
            return resolved.trim();
        }
        if (properties != null && applicationContext != null) {
            return properties.resolvePrefix(applicationContext.getEnvironment());
        } else if (properties != null) {
            return properties.resolvePrefix(null);
        }
        return io.oxmq.client.QueueKeys.resolvePrefix(null);
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> targetClass = AopUtils.getTargetClass(bean);

        // 1. Check for Class-level @OxmqListener with @OxmqHandler methods
        if (targetClass.isAnnotationPresent(OxmqListener.class)) {
            OxmqListener classListener = targetClass.getAnnotation(OxmqListener.class);
            registerClassLevelListener(bean, targetClass, classListener);
        }

        // 2. Discover Method-level @OxmqListener or @OxListener
        var annotatedMethods = MethodIntrospector.selectMethods(targetClass,
                (MethodIntrospector.MetadataLookup<OxmqListener>) method -> {
                    OxmqListener oxmqListener = AnnotatedElementUtils.findMergedAnnotation(method, OxmqListener.class);
                    if (oxmqListener != null) return oxmqListener;
                    OxListener oxListener = AnnotatedElementUtils.findMergedAnnotation(method, OxListener.class);
                    if (oxListener != null) {
                        return new OxmqListener() {
                            public Class<? extends java.lang.annotation.Annotation> annotationType() { return OxmqListener.class; }
                            public String prefix() { return ""; }
                            public String queue() { return oxListener.queue(); }
                            public int concurrency() { return oxListener.concurrency(); }
                            public String concurrencyString() { return ""; }
                            public boolean virtualThreads() { return oxListener.virtualThreads() && oxListener.useVirtualThreads(); }
                            public long lockDurationMs() { return oxListener.lockDurationMs(); }
                            public long pollIntervalMs() { return oxListener.pollIntervalMs(); }
                            public int rateLimitMax() { return oxListener.rateLimitMax(); }
                            public long rateLimitDurationMs() { return oxListener.rateLimitDurationMs(); }
                        };
                    }
                    return null;
                });

        if (!annotatedMethods.isEmpty()) {
            log.debug("Found {} @OxmqListener method(s) in bean {}", annotatedMethods.size(), beanName);
        }
        for (var entry : annotatedMethods.entrySet()) {
            Method method = entry.getKey();
            OxmqListener listener = entry.getValue();
            registerMethodLevelListener(bean, method, listener);
        }

        // 3. Discover Method-level @OxmqBatchListener
        var batchMethods = MethodIntrospector.selectMethods(targetClass,
                (MethodIntrospector.MetadataLookup<OxmqBatchListener>) method ->
                        AnnotatedElementUtils.findMergedAnnotation(method, OxmqBatchListener.class));

        if (!batchMethods.isEmpty()) {
            log.debug("Found {} @OxmqBatchListener method(s) in bean {}", batchMethods.size(), beanName);
        }
        for (var entry : batchMethods.entrySet()) {
            Method method = entry.getKey();
            OxmqBatchListener batchListener = entry.getValue();
            registerBatchListenerMethod(bean, method, batchListener);
        }

        return bean;
    }

    private synchronized void resolveDependenciesIfNecessary() {
        if (this.applicationContext != null) {
            if (this.redisClient == null) {
                try {
                    this.redisClient = applicationContext.getBean("oxmqRedisClient", RedisClient.class);
                } catch (Exception e1) {
                    try {
                        this.redisClient = applicationContext.getBean(RedisClient.class);
                    } catch (Exception ignored) {}
                }
            }

            if (this.scriptManager == null) {
                try {
                    this.scriptManager = applicationContext.getBean(LuaScriptManager.class);
                } catch (Exception ignored) {
                    this.scriptManager = new LuaScriptManager();
                }
            }

            if (this.serializer == null) {
                try {
                    this.serializer = applicationContext.getBean(JobSerializer.class);
                } catch (Exception ignored) {
                    this.serializer = new JacksonJobSerializer();
                }
            }

            if (this.metrics == null) {
                try {
                    this.metrics = applicationContext.getBean(OxmqMetrics.class);
                } catch (Exception ignored) {
                    this.metrics = new OxmqMetrics(null);
                }
            }

            if (this.connectionManager == null) {
                try {
                    this.connectionManager = applicationContext.getBean(RedisConnectionManager.class);
                } catch (Exception ignored) {
                    if (this.redisClient != null) {
                        this.connectionManager = new RedisConnectionManager(redisClient);
                    }
                }
            }

            if (this.properties == null) {
                try {
                    this.properties = applicationContext.getBean(OxmqProperties.class);
                } catch (Exception ignored) {
                    this.properties = new OxmqProperties();
                }
            }
        }
    }

    private void registerMethodLevelListener(Object bean, Method method, OxmqListener listener) {
        resolveDependenciesIfNecessary();
        if (redisClient == null) {
            log.warn("Cannot register @OxmqListener for method {}: RedisClient bean is not available", method);
            return;
        }

        String queueName = resolve(listener.queue());
        String prefix = resolveEffectivePrefix(listener.prefix());
        int concurrency = resolveConcurrency(listener);
        boolean useVirtualThreads = listener.virtualThreads();
        long lockDurationMs = listener.lockDurationMs();
        long pollIntervalMs = listener.pollIntervalMs();

        method.setAccessible(true);
        Class<?> payloadClass = determinePayloadClass(method);

        var builder = OxmqWorker.builder()
                .prefix(prefix)
                .queueName(queueName)
                .connectionManager(connectionManager != null ? connectionManager : new RedisConnectionManager(redisClient))
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

        builder.processor(job -> invokeListenerMethod(bean, method, job));

        Worker<?> worker = builder.build();
        registeredWorkers.add(worker);
        if (running) {
            worker.start();
        }
        log.info("Registered @OxmqListener worker for queue '{}:{}' -> {}.{}()", prefix, queueName, bean.getClass().getSimpleName(), method.getName());
    }

    private record HandlerMethod(Method method, Class<?> payloadType, String name, boolean isDefault) {}

    private void registerClassLevelListener(Object bean, Class<?> targetClass, OxmqListener listener) {
        resolveDependenciesIfNecessary();
        if (redisClient == null) {
            log.warn("Cannot register class-level @OxmqListener for class {}: RedisClient bean is not available", targetClass);
            return;
        }

        String queueName = resolve(listener.queue());
        String prefix = resolveEffectivePrefix(listener.prefix());
        int concurrency = resolveConcurrency(listener);
        boolean useVirtualThreads = listener.virtualThreads();

        List<HandlerMethod> handlers = new ArrayList<>();
        HandlerMethod defaultHandler = null;

        for (Method method : targetClass.getDeclaredMethods()) {
            OxmqHandler handlerAnn = AnnotatedElementUtils.findMergedAnnotation(method, OxmqHandler.class);
            if (handlerAnn != null) {
                method.setAccessible(true);
                Class<?> payloadType = determinePayloadClass(method);
                HandlerMethod hm = new HandlerMethod(method, payloadType, handlerAnn.name().trim(), handlerAnn.isDefault());
                if (handlerAnn.isDefault()) {
                    defaultHandler = hm;
                } else {
                    handlers.add(hm);
                }
            }
        }

        if (handlers.isEmpty() && defaultHandler == null) {
            return;
        }

        final HandlerMethod finalDefaultHandler = defaultHandler;

        var builder = OxmqWorker.builder()
                .prefix(prefix)
                .queueName(queueName)
                .connectionManager(connectionManager != null ? connectionManager : new RedisConnectionManager(redisClient))
                .scriptManager(scriptManager)
                .serializer(serializer)
                .metrics(metrics)
                .concurrency(concurrency)
                .useVirtualThreads(useVirtualThreads)
                .lockDurationMs(listener.lockDurationMs())
                .pollIntervalMs(listener.pollIntervalMs());

        builder.processor(job -> {
            log.info("ClassListener received job '{}' with data: {}", job.getName(), job.getData());
            try {
                Object data = job.getData();
                Method targetMethod = null;

                // 1. Exact name match on @OxmqHandler(name = "...")
                for (HandlerMethod hm : handlers) {
                    if (!hm.name().isEmpty() && hm.name().equalsIgnoreCase(job.getName())) {
                        targetMethod = hm.method();
                        break;
                    }
                }

                // 2. Name match against payload class simple name or method name
                if (targetMethod == null && job.getName() != null) {
                    for (HandlerMethod hm : handlers) {
                        if (hm.payloadType() != null) {
                            String typeName = hm.payloadType().getSimpleName();
                            if (typeName.equalsIgnoreCase(job.getName())
                                    || typeName.toLowerCase().startsWith(job.getName().toLowerCase())
                                    || hm.method().getName().toLowerCase().contains(job.getName().toLowerCase())) {
                                targetMethod = hm.method();
                                break;
                            }
                        }
                    }
                }

                // 3. Type assignment or conversion match
                if (targetMethod == null && data != null) {
                    for (HandlerMethod hm : handlers) {
                        if (hm.payloadType() != null && hm.payloadType().isInstance(data)) {
                            targetMethod = hm.method();
                            break;
                        }
                    }
                }

                // 4. Default handler fallback
                if (targetMethod == null && finalDefaultHandler != null) {
                    targetMethod = finalDefaultHandler.method();
                }

                if (targetMethod == null) {
                    throw new IllegalStateException("No matching @OxmqHandler found on " + bean.getClass().getSimpleName()
                            + " for job '" + job.getName() + "' and payload type: " + (data != null ? data.getClass().getName() : "null"));
                }
                log.info("Invoking handler method {}.{}() for job '{}'", bean.getClass().getSimpleName(), targetMethod.getName(), job.getName());
                return invokeListenerMethod(bean, targetMethod, job);
            } catch (Throwable t) {
                log.error("Exception in class-level listener processor for job '{}'", job.getName(), t);
                throw t;
            }
        });

        Worker<?> worker = builder.build();
        registeredWorkers.add(worker);
        if (running) {
            worker.start();
        }
        log.info("Registered class-level @OxmqListener with {} @OxmqHandler(s) for queue '{}:{}' -> {}",
                handlers.size(), prefix, queueName, bean.getClass().getSimpleName());
    }

    private void registerBatchListenerMethod(Object bean, Method method, OxmqBatchListener batchListener) {
        resolveDependenciesIfNecessary();
        if (redisClient == null) {
            log.warn("Cannot register @OxmqBatchListener for method {}: RedisClient bean is not available", method);
            return;
        }

        String queueName = resolve(batchListener.queue());
        String prefix = resolveEffectivePrefix(batchListener.prefix());
        int batchSize = batchListener.batchSize();
        if (batchListener.batchSizeString() != null && !batchListener.batchSizeString().isBlank()) {
            try {
                batchSize = Integer.parseInt(resolve(batchListener.batchSizeString()));
            } catch (NumberFormatException ignored) {}
        }

        int concurrency = batchListener.concurrency();
        if (batchListener.concurrencyString() != null && !batchListener.concurrencyString().isBlank()) {
            try {
                concurrency = Integer.parseInt(resolve(batchListener.concurrencyString()));
            } catch (NumberFormatException ignored) {}
        }

        method.setAccessible(true);
        Parameter[] params = method.getParameters();

        Class<?> payloadClass = null;
        boolean expectsJobs = false;
        if (params.length == 1) {
            Class<?> paramType = params[0].getType();
            if (List.class.isAssignableFrom(paramType)) {
                Type genericType = params[0].getParameterizedType();
                if (genericType instanceof ParameterizedType pt) {
                    Type[] typeArgs = pt.getActualTypeArguments();
                    if (typeArgs.length > 0) {
                        Type itemType = typeArgs[0];
                        if (itemType instanceof ParameterizedType pt2) {
                            expectsJobs = Job.class.isAssignableFrom((Class<?>) pt2.getRawType());
                            Type[] jobArgs = pt2.getActualTypeArguments();
                            if (jobArgs.length > 0 && jobArgs[0] instanceof Class<?> clazz) {
                                payloadClass = clazz;
                            }
                        } else if (itemType instanceof Class<?> clazz) {
                            if (Job.class.isAssignableFrom(clazz)) {
                                expectsJobs = true;
                            } else {
                                payloadClass = clazz;
                            }
                        }
                    }
                }
            }
        }

        final Class<?> targetPayloadClass = payloadClass;
        final boolean finalExpectsJobs = expectsJobs;

        var builder = OxmqBatchWorker.builder()
                .prefix(prefix)
                .queueName(queueName)
                .batchSize(batchSize)
                .concurrency(concurrency)
                .connectionManager(connectionManager != null ? connectionManager : new RedisConnectionManager(redisClient))
                .scriptManager(scriptManager)
                .serializer(serializer)
                .metrics(metrics)
                .useVirtualThreads(batchListener.virtualThreads())
                .lockDurationMs(batchListener.lockDurationMs())
                .pollIntervalMs(batchListener.pollIntervalMs());

        if (payloadClass != null) {
            builder.payloadClass((Class) payloadClass);
        }

        builder.processor(jobs -> {
            try {
                if (params.length == 0) {
                    return method.invoke(bean);
                } else if (params.length == 1) {
                    Class<?> paramType = params[0].getType();
                    if (List.class.isAssignableFrom(paramType)) {
                        if (finalExpectsJobs) {
                            if (targetPayloadClass != null) {
                                List<Job<Object>> typedJobs = new ArrayList<>(jobs.size());
                                for (Job<?> j : jobs) {
                                    Object converted = convertPayload(j.getData(), targetPayloadClass);
                                    Job<Object> tj = new Job<>(j.getId(), j.getName(), converted, j.getOpts());
                                    tj.setTimestamp(j.getTimestamp());
                                    tj.setAttemptsMade(j.getAttemptsMade());
                                    tj.setParentKey(j.getParentKey());
                                    tj.setQueueName(j.getQueueName());
                                    typedJobs.add(tj);
                                }
                                return method.invoke(bean, typedJobs);
                            }
                            return method.invoke(bean, jobs);
                        } else {
                            List<Object> payloads = new ArrayList<>(jobs.size());
                            for (Job<?> j : jobs) {
                                Object data = targetPayloadClass != null ? convertPayload(j.getData(), targetPayloadClass) : j.getData();
                                payloads.add(data);
                            }
                            return method.invoke(bean, payloads);
                        }
                    } else {
                        return method.invoke(bean, jobs);
                    }
                } else {
                    throw new IllegalArgumentException("@OxmqBatchListener method must have 0 or 1 parameter: " + method);
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
        if (running) {
            worker.start();
        }
        log.info("Registered @OxmqBatchListener worker for queue '{}:{}' (batchSize: {}) -> {}.{}()",
                prefix, queueName, batchSize, bean.getClass().getSimpleName(), method.getName());
    }

    private Object invokeListenerMethod(Object bean, Method method, Job<?> job) throws Exception {
        try {
            Parameter[] parameters = method.getParameters();
            if (parameters.length == 0) {
                return method.invoke(bean);
            }

            Object[] args = new Object[parameters.length];
            for (int i = 0; i < parameters.length; i++) {
                args[i] = resolveParameterArgument(parameters[i], job);
            }

            return method.invoke(bean, args);
        } catch (java.lang.reflect.InvocationTargetException ite) {
            Throwable target = ite.getTargetException();
            if (target instanceof Exception ex) throw ex;
            if (target instanceof Error err) throw err;
            throw ite;
        }
    }

    private Object resolveParameterArgument(Parameter parameter, Job<?> job) {
        Header header = parameter.getAnnotation(Header.class);
        if (header != null) {
            String name = header.value();
            if (name.equalsIgnoreCase("id") || name.equalsIgnoreCase("jobId")) {
                return job.getId();
            } else if (name.equalsIgnoreCase("name") || name.equalsIgnoreCase("jobName")) {
                return job.getName();
            } else if (name.equalsIgnoreCase("timestamp")) {
                return job.getTimestamp();
            } else if (name.equalsIgnoreCase("attemptsMade") || name.equalsIgnoreCase("attempts")) {
                return job.getAttemptsMade();
            } else if (name.equalsIgnoreCase("queue") || name.equalsIgnoreCase("queueName")) {
                return job.getQueueName();
            } else if (name.equalsIgnoreCase("parentKey")) {
                return job.getParentKey();
            }
        }

        Class<?> type = parameter.getType();
        if (Job.class.isAssignableFrom(type)) {
            return job;
        }

        Object data = job.getData();
        if (data == null) {
            return null;
        }

        if (type.isInstance(data)) {
            return data;
        }

        return convertPayload(data, type);
    }

    private Object convertPayload(Object data, Class<?> targetClass) {
        if (data == null || targetClass == null || targetClass.isInstance(data)) {
            return data;
        }
        if (data instanceof String str) {
            return serializer.deserialize(str, targetClass);
        }
        if (serializer instanceof JacksonJobSerializer jackson) {
            return jackson.getObjectMapper().convertValue(data, targetClass);
        }
        String json = serializer.serialize(data);
        return serializer.deserialize(json, targetClass);
    }

    private Class<?> determinePayloadClass(Method method) {
        Parameter[] params = method.getParameters();
        for (Parameter param : params) {
            if (param.getAnnotation(Header.class) != null) continue;
            Class<?> type = param.getType();
            if (Job.class.isAssignableFrom(type)) {
                Type genericType = param.getParameterizedType();
                if (genericType instanceof ParameterizedType pt) {
                    Type[] typeArgs = pt.getActualTypeArguments();
                    if (typeArgs.length > 0 && typeArgs[0] instanceof Class<?> clazz) {
                        return clazz;
                    }
                }
            } else {
                return type;
            }
        }
        return null;
    }

    private int resolveConcurrency(OxmqListener listener) {
        if (listener.concurrencyString() != null && !listener.concurrencyString().isBlank()) {
            try {
                return Integer.parseInt(resolve(listener.concurrencyString()));
            } catch (NumberFormatException ignored) {}
        }
        return listener.concurrency() > 0 ? listener.concurrency() : (properties != null ? properties.getDefaultConcurrency() : 20);
    }

    @Override
    public synchronized void start() {
        if (!running) {
            log.info("Starting {} OxMQ listener worker(s)...", registeredWorkers.size());
            for (Worker<?> worker : registeredWorkers) {
                worker.start();
            }
            running = true;
        }
    }

    @Override
    public synchronized void stop() {
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

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 1000;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    public List<Worker<?>> getRegisteredWorkers() {
        return registeredWorkers;
    }
}
