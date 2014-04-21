﻿using System.Diagnostics;
using NUnit.Framework;

namespace metrics.Tests.Support
{
    [TestFixture]
    public class RandomTests
    {
        [Test]
        public void Can_generate_random_longs()
        {
            for(var i = 0; i < 1000; i++)
            {
                var random = metrics.Support.Random.NextInt();
                Trace.WriteLine(random);
            }
        }
    }
}
