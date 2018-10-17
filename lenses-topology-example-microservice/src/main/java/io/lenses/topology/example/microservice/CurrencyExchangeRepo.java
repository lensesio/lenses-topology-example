package io.lenses.topology.example.microservice;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Contains exchange rates
 */
public class CurrencyExchangeRepo {
  private final Map<String, BigDecimal> currencyXcg = new HashMap<String, BigDecimal>();

  public CurrencyExchangeRepo() {
    currencyXcg.put("GBP", new BigDecimal("1.00"));
    currencyXcg.put("USD", new BigDecimal("1.313558"));
    currencyXcg.put("EUR", new BigDecimal("1.144073"));
    currencyXcg.put("CAN", new BigDecimal("1.702642"));
    currencyXcg.put("CHF", new BigDecimal("1.303682"));
  }

  public BigDecimal getExchangeRate(String currency) {
    return currencyXcg.get(currency);
  }

  public Set<String> getCurrencies() {
    return currencyXcg.keySet();
  }
}
