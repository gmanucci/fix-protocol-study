namespace Fix.Consumer;

/// <summary>
/// Indirection that lets <c>MarketConnectionFactory</c> create QuickFIX-backed connections
/// without taking a hard compile-time dependency on the QuickFIX/n library. Implemented by
/// <c>Fix.Consumer.QuickFix.QuickFixConnectionProvider</c> and registered into DI by
/// <c>AddQuickFixConsumer()</c>.
/// </summary>
public interface IQuickFixConnectionProvider
{
    IMarketConnection Create(string symbol, ConsumerOptions options);
}
