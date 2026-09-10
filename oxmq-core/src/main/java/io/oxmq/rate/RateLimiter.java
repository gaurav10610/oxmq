package io.oxmq.rate;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.oxmq.lua.LuaScript;
import io.oxmq.lua.LuaScriptManager;
import java.time.Duration;

/**
 * Sliding window token bucket rate limiter backed by Redis Lua script.
 */
public class RateLimiter {

    private final StatefulRedisConnection<String, String> connection;
    private final LuaScriptManager scriptManager;
    private final String limiterKey;
    private final String metaKey;
    private final int max;
    private final long durationMs;

    public RateLimiter(StatefulRedisConnection<String, String> connection, LuaScriptManager scriptManager,
                       String queuePrefix, int max, Duration duration) {
        this.connection = connection;
        this.scriptManager = scriptManager;
        this.limiterKey = queuePrefix + ":limiter";
        this.metaKey = queuePrefix + ":meta";
        this.max = max;
        this.durationMs = duration.toMillis();
    }

    /**
     * Checks and consumes a token within the sliding window using BullMQ rate limiting semantics.
     *
     * @return true if token was acquired and job execution is allowed, false if rate limited
     */
    public boolean tryAcquire() {
        Long ttl = scriptManager.eval(connection, LuaScript.GET_RATE_LIMIT_TTL, ScriptOutputType.INTEGER,
                new String[]{limiterKey, metaKey},
                String.valueOf(max)
        );
        return ttl == null || ttl <= 0;
    }

    public int getMax() {
        return max;
    }

    public long getDurationMs() {
        return durationMs;
    }
}
