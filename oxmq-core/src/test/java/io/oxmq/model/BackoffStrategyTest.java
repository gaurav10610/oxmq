package io.oxmq.model;

import java.time.Duration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BackoffStrategyTest {

    @Test
    void testFixedBackoff() {
        BackoffStrategy fixed = BackoffStrategy.fixed(Duration.ofSeconds(3));
        assertThat(fixed.calculateDelayMs(1)).isEqualTo(3000);
        assertThat(fixed.calculateDelayMs(2)).isEqualTo(3000);
        assertThat(fixed.calculateDelayMs(10)).isEqualTo(3000);
    }

    @Test
    void testExponentialBackoff() {
        BackoffStrategy exp = BackoffStrategy.exponential(Duration.ofSeconds(1), Duration.ofSeconds(10));
        // attempt 1: 1000 * 2^0 = 1000
        assertThat(exp.calculateDelayMs(1)).isEqualTo(1000);
        // attempt 2: 1000 * 2^1 = 2000
        assertThat(exp.calculateDelayMs(2)).isEqualTo(2000);
        // attempt 3: 1000 * 2^2 = 4000
        assertThat(exp.calculateDelayMs(3)).isEqualTo(4000);
        // attempt 4: 1000 * 2^3 = 8000
        assertThat(exp.calculateDelayMs(4)).isEqualTo(8000);
        // attempt 5: 1000 * 2^4 = 16000 -> capped at 10000
        assertThat(exp.calculateDelayMs(5)).isEqualTo(10000);
        // attempt 10: capped at 10000
        assertThat(exp.calculateDelayMs(10)).isEqualTo(10000);
    }
}
