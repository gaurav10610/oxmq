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
    @DisplayName("parse: parses day-of-week range ending in 7 (Sunday) correctly")
    void testDayOfWeekRangeEndingInSeven() {
        // "0 12 * * 1-7" (every day Mon-Sun at 12:00)
        CronEvaluator evaluator = CronEvaluator.parse("0 12 * * 1-7");
        assertNotNull(evaluator);

        // Saturday Oct 24, 2026 at 13:00 -> next is Sunday Oct 25 at 12:00
        ZonedDateTime saturday = ZonedDateTime.of(2026, 10, 24, 13, 0, 0, 0, ZoneId.of("UTC"));
        long next = evaluator.nextExecution(saturday.toInstant().toEpochMilli(), "UTC");
        ZonedDateTime nextDt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(next), ZoneId.of("UTC"));

        assertEquals(25, nextDt.getDayOfMonth()); // Sunday
        assertEquals(12, nextDt.getHour());

        // "0 12 * * 5-7" (Friday to Sunday)
        CronEvaluator weekendEvaluator = CronEvaluator.parse("0 12 * * 5-7");
        long nextWeekend = weekendEvaluator.nextExecution(saturday.toInstant().toEpochMilli(), "UTC");
        ZonedDateTime nextWeekendDt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(nextWeekend), ZoneId.of("UTC"));
        assertEquals(25, nextWeekendDt.getDayOfMonth()); // Sunday
        assertEquals(12, nextWeekendDt.getHour());
    }

    @Test
    @DisplayName("parse: supports named days and months case-insensitively")
    void testNamedDaysAndMonths() {
        CronEvaluator evaluator = CronEvaluator.parse("0 9 * * MON-FRI");
        assertNotNull(evaluator);

        // Saturday Oct 24, 2026 at 10:00 -> next execution must be Monday Oct 26 at 09:00
        ZonedDateTime saturday = ZonedDateTime.of(2026, 10, 24, 10, 0, 0, 0, ZoneId.of("UTC"));
        long next = evaluator.nextExecution(saturday.toInstant().toEpochMilli(), "UTC");
        ZonedDateTime nextDt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(next), ZoneId.of("UTC"));
        assertEquals(26, nextDt.getDayOfMonth()); // Monday
        assertEquals(9, nextDt.getHour());
        assertEquals(0, nextDt.getMinute());

        // Month name: "0 0 1 JAN *" -> next Jan 1
        CronEvaluator janEvaluator = CronEvaluator.parse("0 0 1 JAN *");
        long nextJan = janEvaluator.nextExecution(saturday.toInstant().toEpochMilli(), "UTC");
        ZonedDateTime nextJanDt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(nextJan), ZoneId.of("UTC"));
        assertEquals(2027, nextJanDt.getYear());
        assertEquals(1, nextJanDt.getMonthValue());
        assertEquals(1, nextJanDt.getDayOfMonth());
    }

    @Test
    @DisplayName("parse: supports wrap-around ranges across weekend and year-end")
    void testWrapAroundRanges() {
        // FRI-SUN (5 to 0)
        CronEvaluator weekend = CronEvaluator.parse("0 12 * * FRI-SUN");
        assertNotNull(weekend);
        // On Thursday Oct 22, 2026 at 10:00 -> next is Friday Oct 23 at 12:00
        ZonedDateTime thurs = ZonedDateTime.of(2026, 10, 22, 10, 0, 0, 0, ZoneId.of("UTC"));
        long nextFri = weekend.nextExecution(thurs.toInstant().toEpochMilli(), "UTC");
        ZonedDateTime nextFriDt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(nextFri), ZoneId.of("UTC"));
        assertEquals(23, nextFriDt.getDayOfMonth()); // Friday
        assertEquals(12, nextFriDt.getHour());

        // On Friday Oct 23 at 13:00 -> next is Saturday Oct 24 at 12:00
        long nextSat = weekend.nextExecution(nextFri, "UTC");
        ZonedDateTime nextSatDt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(nextSat), ZoneId.of("UTC"));
        assertEquals(24, nextSatDt.getDayOfMonth()); // Saturday

        // On Saturday Oct 24 at 13:00 -> next is Sunday Oct 25 at 12:00
        long nextSun = weekend.nextExecution(nextSat, "UTC");
        ZonedDateTime nextSunDt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(nextSun), ZoneId.of("UTC"));
        assertEquals(25, nextSunDt.getDayOfMonth()); // Sunday

        // NOV-FEB wrap-around month range
        CronEvaluator winter = CronEvaluator.parse("0 0 1 NOV-FEB *");
        assertNotNull(winter);
        ZonedDateTime oct = ZonedDateTime.of(2026, 10, 15, 0, 0, 0, 0, ZoneId.of("UTC"));
        long nextNov = winter.nextExecution(oct.toInstant().toEpochMilli(), "UTC");
        ZonedDateTime nextNovDt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(nextNov), ZoneId.of("UTC"));
        assertEquals(11, nextNovDt.getMonthValue());
        assertEquals(1, nextNovDt.getDayOfMonth());

        // On Dec 2, 2026 -> next is Jan 1, 2027
        ZonedDateTime dec = ZonedDateTime.of(2026, 12, 2, 0, 0, 0, 0, ZoneId.of("UTC"));
        long nextJan = winter.nextExecution(dec.toInstant().toEpochMilli(), "UTC");
        ZonedDateTime nextJanDt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(nextJan), ZoneId.of("UTC"));
        assertEquals(2027, nextJanDt.getYear());
        assertEquals(1, nextJanDt.getMonthValue());
    }

    @Test
    @DisplayName("parse: leap year execution on Feb 29")
    void testLeapYearExecution() {
        CronEvaluator leap = CronEvaluator.parse("0 0 29 2 *");
        assertNotNull(leap);
        // Oct 2026 -> next leap year is 2028
        ZonedDateTime now = ZonedDateTime.of(2026, 10, 24, 0, 0, 0, 0, ZoneId.of("UTC"));
        long next = leap.nextExecution(now.toInstant().toEpochMilli(), "UTC");
        ZonedDateTime nextDt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(next), ZoneId.of("UTC"));
        assertEquals(2028, nextDt.getYear());
        assertEquals(2, nextDt.getMonthValue());
        assertEquals(29, nextDt.getDayOfMonth());
        assertEquals(0, nextDt.getHour());
        assertEquals(0, nextDt.getMinute());
    }

    @Test
    @DisplayName("parse: rejects invalid cron patterns")
    void testInvalidCron() {
        assertThrows(IllegalArgumentException.class, () -> CronEvaluator.parse(null));
        assertThrows(IllegalArgumentException.class, () -> CronEvaluator.parse(""));
        assertThrows(IllegalArgumentException.class, () -> CronEvaluator.parse("* * *"));
        assertThrows(IllegalArgumentException.class, () -> CronEvaluator.parse("* * * * * * *"));
    }
}
