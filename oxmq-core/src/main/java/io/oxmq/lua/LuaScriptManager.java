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
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages loading, SHA-1 digest calculation, and high-performance atomic execution of Lua scripts.
 */
public class LuaScriptManager {

    private static final Logger log = LoggerFactory.getLogger(LuaScriptManager.class);

    private static final Map<LuaScript, String> STATIC_SCRIPT_SOURCES = new EnumMap<>(LuaScript.class);
    private static final Map<LuaScript, String> STATIC_SCRIPT_SHAS = new EnumMap<>(LuaScript.class);
    private static final Map<ExtensionLuaScript, String> STATIC_EXTENSION_SOURCES = new EnumMap<>(ExtensionLuaScript.class);
    private static final Map<ExtensionLuaScript, String> STATIC_EXTENSION_SHAS = new EnumMap<>(ExtensionLuaScript.class);

    static {
        loadStaticScripts();
        loadStaticExtensionScripts();
    }

    private final Map<LuaScript, String> scriptSources = STATIC_SCRIPT_SOURCES;
    private final Map<LuaScript, String> scriptShas = STATIC_SCRIPT_SHAS;
    private final Map<ExtensionLuaScript, String> extensionSources = STATIC_EXTENSION_SOURCES;
    private final Map<ExtensionLuaScript, String> extensionShas = STATIC_EXTENSION_SHAS;

    public LuaScriptManager() {
        // Scripts loaded statically once
    }

    private static void loadStaticExtensionScripts() {
        for (ExtensionLuaScript script : ExtensionLuaScript.values()) {
            try (InputStream in = LuaScriptManager.class.getClassLoader().getResourceAsStream(script.getResourcePath())) {
                if (in == null) {
                    throw new IllegalStateException("Missing extension Lua script resource: " + script.getResourcePath());
                }
                String source = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))
                        .lines().collect(Collectors.joining("\n"));
                STATIC_EXTENSION_SOURCES.put(script, source);

                MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
                byte[] hash = sha1.digest(source.getBytes(StandardCharsets.UTF_8));
                String sha = HexFormat.of().formatHex(hash);
                STATIC_EXTENSION_SHAS.put(script, sha);
                log.debug("Loaded extension script {} with SHA-1: {}", script.name(), sha);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to load extension script: " + script.getResourcePath(), e);
            }
        }
    }

    private static void loadStaticScripts() {
        for (LuaScript script : LuaScript.values()) {
            try (InputStream in = LuaScriptManager.class.getClassLoader().getResourceAsStream(script.getResourcePath())) {
                if (in == null) {
                    throw new IllegalStateException("Missing Lua script resource: " + script.getResourcePath());
                }
                String source = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))
                        .lines().collect(Collectors.joining("\n"));
                STATIC_SCRIPT_SOURCES.put(script, source);

                MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
                byte[] hash = sha1.digest(source.getBytes(StandardCharsets.UTF_8));
                String sha = HexFormat.of().formatHex(hash);
                STATIC_SCRIPT_SHAS.put(script, sha);
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

    /**
     * Executes a Lua script asynchronously using EVALSHA with automatic fallback on NOSCRIPT.
     */
    @SuppressWarnings("unchecked")
    public <T> CompletableFuture<T> evalBinaryAsync(StatefulRedisConnection<String, byte[]> connection, LuaScript script,
                                                   ScriptOutputType outputType, String[] keys, byte[]... args) {
        String sha = scriptShas.get(script);
        String source = scriptSources.get(script);
        io.lettuce.core.api.async.RedisAsyncCommands<String, byte[]> commands = connection.async();
        io.lettuce.core.RedisFuture<T> future = (io.lettuce.core.RedisFuture<T>) commands.evalsha(sha, outputType, keys, args);
        return future.toCompletableFuture().exceptionallyCompose(ex -> {
            Throwable cause = ex instanceof java.util.concurrent.CompletionException ? ex.getCause() : ex;
            if (cause != null && cause.getMessage() != null && cause.getMessage().contains("NOSCRIPT")) {
                log.debug("Lua script {} SHA {} not cached in Redis, falling back to EVAL", script.name(), sha);
                try {
                    commands.scriptLoad(source.getBytes(StandardCharsets.UTF_8));
                } catch (Exception ignored) {}
                io.lettuce.core.RedisFuture<T> evalFuture = (io.lettuce.core.RedisFuture<T>) commands.eval(source, outputType, keys, args);
                return evalFuture.toCompletableFuture();
            }
            return CompletableFuture.failedFuture(cause != null ? cause : ex);
        });
    }

    public String getSource(LuaScript script) {
        return scriptSources.get(script);
    }

    public String getSha(LuaScript script) {
        return scriptShas.get(script);
    }

    /**
     * Executes an extension Lua script using EVALSHA with automatic fallback on NOSCRIPT.
     */
    @SuppressWarnings("unchecked")
    public <T> T eval(StatefulRedisConnection<String, String> connection, ExtensionLuaScript script,
                      ScriptOutputType outputType, String[] keys, String... args) {
        String sha = extensionShas.get(script);
        String source = extensionSources.get(script);
        RedisCommands<String, String> commands = connection.sync();

        try {
            return (T) commands.evalsha(sha, outputType, keys, args);
        } catch (RedisException e) {
            if (e.getMessage() != null && e.getMessage().contains("NOSCRIPT")) {
                log.debug("Extension script {} SHA {} not cached in Redis, falling back to EVAL", script.name(), sha);
                try {
                    commands.scriptLoad(source);
                } catch (Exception ignored) {}
                return (T) commands.eval(source, outputType, keys, args);
            }
            throw e;
        }
    }

    /**
     * Executes an extension Lua script with binary arguments using EVALSHA with automatic fallback on NOSCRIPT.
     */
    @SuppressWarnings("unchecked")
    public <T> T evalBinary(StatefulRedisConnection<String, byte[]> connection, ExtensionLuaScript script,
                            ScriptOutputType outputType, String[] keys, byte[]... args) {
        String sha = extensionShas.get(script);
        String source = extensionSources.get(script);
        RedisCommands<String, byte[]> commands = connection.sync();

        try {
            return (T) commands.evalsha(sha, outputType, keys, args);
        } catch (RedisException e) {
            if (e.getMessage() != null && e.getMessage().contains("NOSCRIPT")) {
                log.debug("Extension script {} SHA {} not cached in Redis, falling back to EVAL", script.name(), sha);
                try {
                    commands.scriptLoad(source.getBytes(StandardCharsets.UTF_8));
                } catch (Exception ignored) {}
                return (T) commands.eval(source, outputType, keys, args);
            }
            throw e;
        }
    }

    /**
     * Executes an extension Lua script asynchronously using EVALSHA with automatic fallback on NOSCRIPT.
     */
    @SuppressWarnings("unchecked")
    public <T> CompletableFuture<T> evalBinaryAsync(StatefulRedisConnection<String, byte[]> connection, ExtensionLuaScript script,
                                                            ScriptOutputType outputType, String[] keys, byte[]... args) {
        String sha = extensionShas.get(script);
        String source = extensionSources.get(script);
        io.lettuce.core.api.async.RedisAsyncCommands<String, byte[]> commands = connection.async();
        io.lettuce.core.RedisFuture<T> future = (io.lettuce.core.RedisFuture<T>) commands.evalsha(sha, outputType, keys, args);
        return future.toCompletableFuture().exceptionallyCompose(ex -> {
            Throwable cause = ex instanceof java.util.concurrent.CompletionException ? ex.getCause() : ex;
            if (cause != null && cause.getMessage() != null && cause.getMessage().contains("NOSCRIPT")) {
                log.debug("Extension script {} SHA {} not cached in Redis, falling back to EVAL", script.name(), sha);
                try {
                    commands.scriptLoad(source.getBytes(StandardCharsets.UTF_8));
                } catch (Exception ignored) {}
                io.lettuce.core.RedisFuture<T> evalFuture = (io.lettuce.core.RedisFuture<T>) commands.eval(source, outputType, keys, args);
                return evalFuture.toCompletableFuture();
            }
            return CompletableFuture.failedFuture(cause != null ? cause : ex);
        });
    }

    public String getSource(ExtensionLuaScript script) {
        return extensionSources.get(script);
    }

    public String getSha(ExtensionLuaScript script) {
        return extensionShas.get(script);
    }
}
