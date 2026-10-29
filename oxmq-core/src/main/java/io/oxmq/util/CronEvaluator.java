package io.oxmq.util;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Lightweight, zero-dependency Cron Expression evaluator for recurring BullMQ/OxMQ job schedules.
 * Supports standard 5-field (min hour dom mon dow) and 6-field (sec min hour dom mon dow) cron patterns.
 */
public final class CronEvaluator {

    private final CronField seconds;
    private final CronField minutes;
    private final CronField hours;
    private final CronField dayOfMonth;
    private final CronField month;
    private final CronField dayOfWeek;
    private final boolean hasSeconds;

    private CronEvaluator(CronField seconds, CronField minutes, CronField hours,
                          CronField dayOfMonth, CronField month, CronField dayOfWeek, boolean hasSeconds) {
        this.seconds = seconds;
        this.minutes = minutes;
        this.hours = hours;
        this.dayOfMonth = dayOfMonth;
        this.month = month;
        this.dayOfWeek = dayOfWeek;
        this.hasSeconds = hasSeconds;
    }

    private static final java.util.concurrent.ConcurrentMap<String, CronEvaluator> CACHE = new java.util.concurrent.ConcurrentHashMap<>(64);
    private static final int MAX_CACHE_SIZE = 512;

    public static CronEvaluator parse(String expression) {
        if (expression == null || expression.isBlank()) {
            throw new IllegalArgumentException("Cron expression must not be null or blank");
        }
        String trimmed = expression.trim();
        CronEvaluator cached = CACHE.get(trimmed);
        if (cached != null) {
            return cached;
        }
        CronEvaluator evaluator = parseInternal(trimmed);
        if (CACHE.size() < MAX_CACHE_SIZE) {
            CACHE.putIfAbsent(trimmed, evaluator);
        }
        return evaluator;
    }

    private static CronEvaluator parseInternal(String trimmed) {
        String[] parts = trimmed.split("\\s+");
        if (parts.length != 5 && parts.length != 6) {
            throw new IllegalArgumentException("Cron expression must have 5 or 6 fields, got " + parts.length + ": " + trimmed);
        }

        int idx = 0;
        CronField secField = (parts.length == 6) ? CronField.parse(parts[idx++], 0, 59) : new CronField(Set.of(0));
        CronField minField = CronField.parse(parts[idx++], 0, 59);
        CronField hourField = CronField.parse(parts[idx++], 0, 23);
        CronField domField = CronField.parse(parts[idx++], 1, 31);
        CronField monField = CronField.parse(normalizeMonth(parts[idx++]), 1, 12);
        CronField dowField = CronField.parseDow(normalizeDow(parts[idx++]));

        return new CronEvaluator(secField, minField, hourField, domField, monField, dowField, parts.length == 6);
    }

    private static String normalizeMonth(String part) {
        if (part == null || part.isBlank()) return part;
        return part.toUpperCase(java.util.Locale.ROOT)
                .replace("JAN", "1")
                .replace("FEB", "2")
                .replace("MAR", "3")
                .replace("APR", "4")
                .replace("MAY", "5")
                .replace("JUN", "6")
                .replace("JUL", "7")
                .replace("AUG", "8")
                .replace("SEP", "9")
                .replace("OCT", "10")
                .replace("NOV", "11")
                .replace("DEC", "12");
    }

    private static String normalizeDow(String part) {
        if (part == null || part.isBlank()) return part;
        return part.toUpperCase(java.util.Locale.ROOT)
                .replace("SUN", "0")
                .replace("MON", "1")
                .replace("TUE", "2")
                .replace("WED", "3")
                .replace("THU", "4")
                .replace("FRI", "5")
                .replace("SAT", "6");
    }

    public long nextExecution(long fromMillis, String timezone) {
        ZoneId zone = (timezone != null && !timezone.isBlank()) ? ZoneId.of(timezone) : ZoneId.systemDefault();
        ZonedDateTime current = ZonedDateTime.ofInstant(Instant.ofEpochMilli(fromMillis), zone);

        // Advance by at least 1 unit to avoid re-triggering the current millisecond
        if (hasSeconds) {
            current = current.truncatedTo(ChronoUnit.SECONDS).plusSeconds(1);
        } else {
            current = current.truncatedTo(ChronoUnit.MINUTES).plusMinutes(1);
        }

        // Search up to 5 years into the future
        ZonedDateTime deadline = current.plusYears(5);

        while (current.isBefore(deadline)) {
            if (!month.matches(current.getMonthValue())) {
                current = current.plusMonths(1).withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS);
                continue;
            }
            if (!dayOfMonth.matches(current.getDayOfMonth()) || !dayOfWeek.matches(current.getDayOfWeek().getValue() % 7)) {
                current = current.plusDays(1).truncatedTo(ChronoUnit.DAYS);
                continue;
            }
            if (!hours.matches(current.getHour())) {
                current = current.plusHours(1).truncatedTo(ChronoUnit.HOURS);
                continue;
            }
            if (!minutes.matches(current.getMinute())) {
                current = current.plusMinutes(1).truncatedTo(ChronoUnit.MINUTES);
                continue;
            }
            if (hasSeconds && !seconds.matches(current.getSecond())) {
                current = current.plusSeconds(1);
                continue;
            }
            return current.toInstant().toEpochMilli();
        }

        // Fallback: 60 seconds from fromMillis if no match within 5 years
        return fromMillis + 60_000L;
    }

    private static class CronField {
        private final Set<Integer> allowedValues;

        CronField(Set<Integer> allowedValues) {
            this.allowedValues = allowedValues;
        }

        boolean matches(int val) {
            return allowedValues.contains(val);
        }

        static CronField parse(String part, int min, int max) {
            Set<Integer> values = new HashSet<>();
            for (String sub : part.split(",")) {
                if (sub.equals("*") || sub.equals("?")) {
                    for (int i = min; i <= max; i++) values.add(i);
                } else if (sub.contains("/")) {
                    String[] stepParts = sub.split("/");
                    int start = stepParts[0].equals("*") ? min : Integer.parseInt(stepParts[0]);
                    int step = Integer.parseInt(stepParts[1]);
                    for (int i = start; i <= max; i += step) values.add(i);
                } else if (sub.contains("-")) {
                    String[] rangeParts = sub.split("-");
                    int from = Integer.parseInt(rangeParts[0]);
                    int to = Integer.parseInt(rangeParts[1]);
                    if (from <= to) {
                        for (int i = from; i <= to; i++) values.add(i);
                    } else {
                        // Wrap-around range e.g. 11-2 (NOV-FEB)
                        for (int i = from; i <= max; i++) values.add(i);
                        for (int i = min; i <= to; i++) values.add(i);
                    }
                } else {
                    values.add(Integer.parseInt(sub));
                }
            }
            if (values.isEmpty()) {
                throw new IllegalArgumentException("Invalid cron field: " + part);
            }
            return new CronField(values);
        }

        static CronField parseDow(String part) {
            Set<Integer> values = new HashSet<>();
            for (String sub : part.split(",")) {
                if (sub.equals("*") || sub.equals("?")) {
                    for (int i = 0; i <= 6; i++) values.add(i);
                } else if (sub.contains("/")) {
                    String[] stepParts = sub.split("/");
                    int start = stepParts[0].equals("*") ? 0 : Integer.parseInt(stepParts[0]);
                    int step = Integer.parseInt(stepParts[1]);
                    for (int i = start; i <= 6; i += step) values.add(i % 7);
                } else if (sub.contains("-")) {
                    String[] rangeParts = sub.split("-");
                    int from = Integer.parseInt(rangeParts[0]);
                    int to = Integer.parseInt(rangeParts[1]);
                    if (from <= to) {
                        for (int i = from; i <= to; i++) values.add(i % 7);
                    } else {
                        // Wrap-around range e.g. 5-0 (FRI-SUN) or 5-1 (FRI-MON)
                        for (int i = from; i <= 6; i++) values.add(i % 7);
                        for (int i = 0; i <= to; i++) values.add(i % 7);
                    }
                } else {
                    int val = Integer.parseInt(sub);
                    values.add(val % 7); // 7 is Sunday, same as 0
                }
            }
            if (values.isEmpty()) {
                throw new IllegalArgumentException("Invalid cron day-of-week field: " + part);
            }
            return new CronField(values);
        }
    }
}
