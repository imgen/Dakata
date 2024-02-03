using System;

namespace Dakata.Examples;

public static class Utils
{
    public static DateTime TruncateDateTimeToSeconds(this DateTime time) =>
        new(
            time.Year,
            time.Month,
            time.Day,
            time.Hour,
            time.Minute,
            time.Second,
            time.Kind);
}