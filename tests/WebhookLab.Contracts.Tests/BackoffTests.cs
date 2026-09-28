using WebhookLab.Contracts;

namespace WebhookLab.Contracts.Tests;

public class BackoffTests
{
    [Theory]
    [InlineData(1, 2)]
    [InlineData(2, 4)]
    [InlineData(3, 8)]
    [InlineData(4, 16)]
    public void Delay_doubles_with_each_attempt(int attemptCount, int expectedSeconds)
    {
        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), Backoff.Delay(attemptCount, new FixedRandom(0)));
    }

    [Fact]
    public void Delay_adds_at_most_half_a_second_of_jitter()
    {
        var delay = Backoff.Delay(1, new FixedRandom(0.999));

        Assert.InRange(delay, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(2.5));
    }

    private sealed class FixedRandom(double value) : Random
    {
        public override double NextDouble() => value;
    }
}
