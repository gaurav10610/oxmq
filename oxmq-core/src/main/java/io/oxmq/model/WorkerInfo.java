package io.oxmq.model;

/**
 * Representation of an active worker connected to a queue in the cluster.
 * Parsed from Redis CLIENT LIST for full observability and dashboard monitoring.
 *
 * @param id        the worker unique identifier
 * @param name      the full Redis client name tag (e.g. prefix:queue:w:workerId)
 * @param address   the network address and port of the worker client (addr)
 * @param clientId  the Redis connection client ID (id)
 * @param age       duration the client connection has been active (in seconds)
 * @param idle      duration the client connection has been idle (in seconds)
 * @param flags     Redis client status flags (e.g. N, b)
 */
public record WorkerInfo(
        String id,
        String name,
        String address,
        String clientId,
        String age,
        String idle,
        String flags
) {}
