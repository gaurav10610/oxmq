package io.oxmq.lua;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class LuaScriptManagerTest {

    @Test
    void testAllScriptsLoadAndHaveValidShas() {
        LuaScriptManager manager = new LuaScriptManager();

        for (LuaScript script : LuaScript.values()) {
            String source = manager.getSource(script);
            String sha = manager.getSha(script);

            assertThat(source).isNotNull().isNotBlank();
            assertThat(sha).isNotNull().hasSize(40); // 40-char SHA1 hex
        }
    }
}
