package ru.mail.polis.ivlev;

public final class Time {

    private static int count;
    private static long time;

    private Time() {
    }

    public static long getCurrentTime() {
        final long currentTimeMillis = System.currentTimeMillis();
        if (time != currentTimeMillis) {
            time = currentTimeMillis;
            count = 0;
        }
        return time * 1_000_000 + count++;
    }
}
