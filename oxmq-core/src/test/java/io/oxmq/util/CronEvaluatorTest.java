package io.oxmq.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.*;

class CronEvaluatorTest {

    @Test
    @DisplayName("parse: parses standard 5-part cron expressions correctly")
    void testStandardFiveFieldCron() {
        CronEvaluator evaluator = CronEvaluator.parse("*/5 * * * *");
        assertNotNull(evaluator);

        // Given a time at minute 2 (e.g. 10:02:30), next execution must be at minute 5 (10:05:00)
        ZonedDateTime now = ZonedDateTime.of(2026, 10, 24, 10, 2, 30, 0, ZoneId.of("UTC"));
        long next = evaluator.nextExecution(now.toInstant().toEpochMilli(), "UTC");

        ZonedDateTime nextDt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(next), ZoneId.of("UTC"));
        assertEquals(10, nextDt.getHour());
        assertEquals(5, nextDt.getMinute());
        assertEquals(0, nextDt.getSecond());
    }

    @Test
    @DisplayName("parse: parses midnight cron (0 0 * * *) correctly")
    void testMidnightCron() {
        CronEvaluator evaluator = CronEvaluator.parse("0 0 * * *");
        ZonedDateTime now = ZonedDateTime.of(2026, 10, 24, 15, 30, 0, 0, ZoneId.of("UTC"));
        long next = evaluator.nextExecution(now.toInstant().toEpochMilli(), "UTC");

        ZonedDateTime nextDt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(next), ZoneId.of("UTC"));
        assertEquals(25, nextDt.getDayOfMonth());
        assertEquals(0, nextDt.getHour());
        assertEquals(0, nextDt.getMinute());
    }

    @Test
    @DisplayName("parse: parses 6-part cron with seconds correctly")
    void testSixFieldCron() {
        CronEvaluator evaluator = CronEvaluator.parse("30 */10 * * * *");
        ZonedDateTime now = ZonedDateTime.of(2026, 10, 24, 10, 0, 0, 0, ZoneId.of("UTC"));
        long next = evaluator.nextExecution(now.toInstant().toEpochMilli(), "UTC");

        ZonedDateTime nextDt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(next), ZoneId.of("UTC"));
        assertEquals(0, nextDt.getMinute());
        assertEquals(30, nextDt.getSecond());
    }

    @Test
    @DisplayName("parse: rejects invalid cron patterns")
    void testInvalidCron() {
        assertThrows(IllegalArgumentException.class, () -> CronEvaluator.parse(null));
        assertThrows(IllegalArgumentException.class, () -> CronEvaluator.parse(""));
        assertThrows(IllegalArgumentException.class, () -> CronEvaluator.parse("* * *"));
    }
}
