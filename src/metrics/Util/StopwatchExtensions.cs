using System.Diagnostics;

namespace metrics.Util
{
    public static class StopwatchExtensions
    {
        private const long MaxTickCountConvertibleToNanos = 9223372036L;

        public static long ElapsedNanos(this Stopwatch stopwatch)
        {
            var ticks = stopwatch.ElapsedTicks;
            if (ticks < MaxTickCountConvertibleToNanos)
                return ticks*1000*1000*1000/Stopwatch.Frequency;

            return stopwatch.ElapsedMilliseconds * 1000 * 1000;
        }
    }
}
