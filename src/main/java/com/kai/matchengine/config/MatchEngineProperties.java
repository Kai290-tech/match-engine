package com.kai.matchengine.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "match-engine")
public class MatchEngineProperties {

  private boolean pipelineEnabled = false;
  private Pool pool = new Pool();

  public boolean isPipelineEnabled() {
    return pipelineEnabled;
  }

  public void setPipelineEnabled(boolean pipelineEnabled) {
    this.pipelineEnabled = pipelineEnabled;
  }

  public Pool getPool() {
    return pool;
  }

  public void setPool(Pool pool) {
    this.pool = pool;
  }

  public static class Pool {
    private int orderNodeCapacityPerSymbol = 300_000;

    public int getOrderNodeCapacityPerSymbol() {
      return orderNodeCapacityPerSymbol;
    }

    public void setOrderNodeCapacityPerSymbol(int orderNodeCapacityPerSymbol) {
      this.orderNodeCapacityPerSymbol = orderNodeCapacityPerSymbol;
    }
  }
}
