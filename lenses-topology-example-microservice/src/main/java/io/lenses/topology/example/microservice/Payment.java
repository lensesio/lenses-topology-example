package io.lenses.topology.example.microservice;

import java.math.BigDecimal;

public class Payment {
  private String currency;
  private BigDecimal amount;
  private Long timestamp;

  public Payment(String currency, BigDecimal amount, Long timestamp) {
    this.currency = currency;
    this.amount = amount;
    this.timestamp = timestamp;
  }
  public Payment(){

  }

  public String getCurrency() {
    return currency;
  }

  public void setCurrency(String currency) {
    this.currency = currency;
  }

  public BigDecimal getAmount() {
    return amount;
  }

  public void setAmount(BigDecimal amount) {
    this.amount = amount;
  }

  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }

  public Long getTimestamp() {
    return timestamp;
  }
}
