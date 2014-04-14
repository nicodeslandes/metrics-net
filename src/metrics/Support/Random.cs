using System.Threading;

namespace metrics.Support
{
    /// <summary>
    /// Provides statistically relevant random number generation
    /// </summary>
    internal class Random
    {
		private static readonly ThreadLocal<System.Random> RandomGenerator =
            new ThreadLocal<System.Random>(() => new System.Random());

        public static double NextDouble()
        {
            return RandomGenerator.Value.NextDouble();
        }

        public static int NextInt()
        {
             return RandomGenerator.Value.Next();
        }
    }
}
