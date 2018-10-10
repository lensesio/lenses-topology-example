package io.lenses.topology.example.microservice;

import java.math.BigDecimal;

public class Transaction {
  private String currency;
  private BigDecimal amount;
  private Long timestamp;

  public Transaction(String currency, BigDecimal amount, Long timestamp) {
    this.currency = currency;
    this.amount = amount;
    this.timestamp = timestamp;
  }
  public Transaction(){

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
