package io.oxmq.lua;

import io.lettuce.core.RedisException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.EnumMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages loading, SHA-1 digest calculation, and high-performance atomic execution of Lua scripts.
 */
public class LuaScriptManager {

    private static final Logger log = LoggerFactory.getLogger(LuaScriptManager.class);

    private final Map<LuaScript, String> scriptSources = new EnumMap<>(LuaScript.class);
    private final Map<LuaScript, String> scriptShas = new EnumMap<>(LuaScript.class);

    public LuaScriptManager() {
        loadAllScripts();
    }

    private void loadAllScripts() {
        for (LuaScript script : LuaScript.values()) {
            try (InputStream in = getClass().getClassLoader().getResourceAsStream(script.getResourcePath())) {
                if (in == null) {
                    throw new IllegalStateException("Missing Lua script resource: " + script.getResourcePath());
                }
                String source = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))
                        .lines().collect(Collectors.joining("\n"));
                scriptSources.put(script, source);

                MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
                byte[] hash = sha1.digest(source.getBytes(StandardCharsets.UTF_8));
                String sha = HexFormat.of().formatHex(hash);
                scriptShas.put(script, sha);
                log.debug("Loaded script {} with SHA-1: {}", script.name(), sha);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to load script: " + script.getResourcePath(), e);
            }
        }
    }

    /**
     * Executes a Lua script using EVALSHA with automatic fallback on NOSCRIPT.
     */
    @SuppressWarnings("unchecked")
    public <T> T eval(StatefulRedisConnection<String, String> connection, LuaScript script,
                      ScriptOutputType outputType, String[] keys, String... args) {
        String sha = scriptShas.get(script);
        String source = scriptSources.get(script);
        RedisCommands<String, String> commands = connection.sync();

        try {
            return (T) commands.evalsha(sha, outputType, keys, args);
        } catch (RedisException e) {
            if (e.getMessage() != null && e.getMessage().contains("NOSCRIPT")) {
                log.debug("Script {} SHA {} not cached in Redis, falling back to EVAL", script.name(), sha);
                try {
                    commands.scriptLoad(source);
                } catch (Exception ignored) {}
                return (T) commands.eval(source, outputType, keys, args);
            }
            throw e;
        }
    }

    /**
     * Executes a Lua script with binary arguments using EVALSHA with automatic fallback on NOSCRIPT.
     */
    @SuppressWarnings("unchecked")
    public <T> T evalBinary(StatefulRedisConnection<String, byte[]> connection, LuaScript script,
                            ScriptOutputType outputType, String[] keys, byte[]... args) {
        String sha = scriptShas.get(script);
        String source = scriptSources.get(script);
        RedisCommands<String, byte[]> commands = connection.sync();

        try {
            return (T) commands.evalsha(sha, outputType, keys, args);
        } catch (RedisException e) {
            if (e.getMessage() != null && e.getMessage().contains("NOSCRIPT")) {
                log.debug("Script {} SHA {} not cached in Redis, falling back to EVAL", script.name(), sha);
                try {
                    commands.scriptLoad(source.getBytes(StandardCharsets.UTF_8));
                } catch (Exception ignored) {}
                return (T) commands.eval(source, outputType, keys, args);
            }
            throw e;
        }
    }

    public String getSource(LuaScript script) {
        return scriptSources.get(script);
    }

    public String getSha(LuaScript script) {
        return scriptShas.get(script);
    }
}
