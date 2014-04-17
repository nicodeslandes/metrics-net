using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Serialization;
using metrics.Support;
using metrics.Util;

namespace metrics.Stats
{
    /// <summary>
    /// An exponentially-decaying random sample of {@code long}s. Uses Cormode et
    /// al's forward-decaying priority reservoir sampling method to produce a
    /// statistically representative sample, exponentially biased towards newer
    /// entries.
    /// </summary>
    /// <see href="http://www.research.att.com/people/Cormode_Graham/library/publications/CormodeShkapenyukSrivastavaXu09.pdf">
    /// Cormode et al. Forward Decay: A Practical Time Decay Model for Streaming
    /// Systems. ICDE '09: Proceedings of the 2009 IEEE International Conference on
    /// Data Engineering (2009)
    /// </see>
    public class ExponentiallyDecayingSample : ISample<ExponentiallyDecayingSample>
    {
        private const int DateTimeTicksPerSeconds = 10000000;
        private static readonly long RescaleThreshold = TimeUnit.Hours.ToNanos(1);

        /* Implemented originally as ConcurrentSkipListMap, so lookups will be much slower */
        private readonly SortedDictionary<double, long> _values;
        private readonly object _lock = new object();
        private readonly int _reservoirSize;
        private readonly double _alpha;
        private readonly AtomicLong _count = new AtomicLong(0);
        private VolatileLong _startTimeInSeconds;
        private readonly AtomicLong _nextScaleTimeNanos = new AtomicLong(0);
        private readonly Stopwatch _stopwatch = Stopwatch.StartNew();

        /// <param name="reservoirSize">The number of samples to keep in the sampling reservoir</param>
        /// <param name="alpha">The exponential decay factor; the higher this is, the more biased the sample will be towards newer values</param>
        public ExponentiallyDecayingSample(int reservoirSize, double alpha)
        {
            _values = new SortedDictionary<double, long>();
            _alpha = alpha;
            _reservoirSize = reservoirSize;
            Clear();
        }

        /// <summary>
        /// Clears all recorded values
        /// </summary>
        public void Clear()
        {
            _values.Clear();
            _count.Set(0);
            _startTimeInSeconds = CurrentTimeInSeconds();
        }
        
        /// <summary>
        /// Returns the number of values recorded
        /// </summary>
        public int Count
        {
            get { return (int) Math.Min(_reservoirSize, _count); }
        }

        /// <summary>
        /// Adds a new recorded value to the sample
        /// </summary>
        public void Update(long value)
        {
            Update(value, CurrentTimeInSeconds());
        }

        private void Update(long value, long timestamp)
        {
            // WARNING! This used to be a ReadLock
            // But SortedDictionary isn't thread-safe, so we now have to take a full lock
            lock(_lock)
            {
                var priority = Weight(timestamp - _startTimeInSeconds) / Support.Random.NextDouble();
                var newCount = _count.IncrementAndGet();
                if(newCount <= _reservoirSize)
                {
                    _values[priority] = value;
                }
                else
                {
                    var first = _values.Keys.First();
                    if(first < priority)
                    {
                        _values[priority] = value;
                        _values.Remove(first);
                    }
                }
            }

            var now = _stopwatch.ElapsedNanos();
            var next = _nextScaleTimeNanos.Get();
            if(now >= next)
            {
                Rescale(now, next);
            }
        }

        /// <summary>
        ///  Returns a copy of the sample's values
        /// </summary>
        public ICollection<long> Values
        {
            get
            {
                // WARNING! This used to be a ReadLock
                // But SortedDictionary isn't thread-safe, so we now have to take a full lock
                lock(_lock)
                {
                    return new List<long>(_values.Values);
                }
            }
        }

        private long CurrentTimeInSeconds()
        {
            return DateTime.Now.Ticks / DateTimeTicksPerSeconds;
        }

        private double Weight(long t)
        {
            return Math.Exp(_alpha * t);
        }
        
        /// <summary>
        /// "A common feature of the above techniques—indeed, the key technique that
        /// allows us to track the decayed weights efficiently—is that they maintain
        /// counts and other quantities based on g(ti − L), and only scale by g(t − L)
        /// at query time. But while g(ti −L)/g(t−L) is guaranteed to lie between zero
        /// and one, the intermediate values of g(ti − L) could become very large. For
        /// polynomial functions, these values should not grow too large, and should be
        /// effectively represented in practice by floating point values without loss of
        /// precision. For exponential functions, these values could grow quite large as
        /// new values of (ti − L) become large, and potentially exceed the capacity of
        /// common floating point types. However, since the values stored by the
        /// algorithms are linear combinations of g values (scaled sums), they can be
        /// rescaled relative to a new landmark. That is, by the analysis of exponential
        /// decay in Section III-A, the choice of L does not affect the final result. We
        /// can therefore multiply each value based on L by a factor of exp(−α(L′ − L)),
        /// and obtain the correct value as if we had instead computed relative to a new
        /// landmark L′ (and then use this new L′ at query time). This can be done with
        /// a linear pass over whatever data structure is being used."
        /// </summary>
        /// <param name="now"></param>
        /// <param name="next"></param>
        private void Rescale(long now, long next)
        {
            if (!_nextScaleTimeNanos.CompareAndSet(next, now + RescaleThreshold))
            {
                return;
            }

            lock(_lock)
            {
                var oldStartTime = _startTimeInSeconds;
                _startTimeInSeconds = CurrentTimeInSeconds();
                var keys = new List<double>(_values.Keys);
                foreach (var key in keys)
                {
                    long value = _values[key];
                    _values.Remove(key);
                    _values[key * Math.Exp(-_alpha * (_startTimeInSeconds - oldStartTime))] = value;
                }
            }
        }

        /// <summary>
        /// Returns a Copy of the current sample
        /// </summary>
        [IgnoreDataMember]
        public ExponentiallyDecayingSample Copy
        {
            get
            {
                var copy = new ExponentiallyDecayingSample(_reservoirSize, _alpha);
                copy._startTimeInSeconds.Set(_startTimeInSeconds);
                copy._count.Set(_count);
                copy._nextScaleTimeNanos.Set(_nextScaleTimeNanos);
                foreach(var value in _values)
                {
                    copy._values[value.Key] = value.Value;
                }
                return copy;
            }
        }
    }
}