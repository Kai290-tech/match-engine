package com.kai.matchengine.config;

import com.kai.matchengine.outbound.OutboundTopics;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "outbound")
public class OutboundProperties {

  /** false 时不创建 Producer/不启动 Publisher（由 pipeline 显式组装时可仍注入 Topics）。 */
  private boolean enabled = false;
  private Kafka kafka = new Kafka();
  /** sbe | json */
  private String format = "sbe";
  private int queueCapacity = 65536;
  private long sendTimeoutMs = 30_000L;
  private Backpressure backpressure = new Backpressure();

  public Kafka getKafka() {
    return kafka;
  }

  public void setKafka(Kafka kafka) {
    this.kafka = kafka;
  }

  public String getFormat() {
    return format;
  }

  public void setFormat(String format) {
    this.format = format;
  }

  public int getQueueCapacity() {
    return queueCapacity;
  }

  public void setQueueCapacity(int queueCapacity) {
    this.queueCapacity = queueCapacity;
  }

  public long getSendTimeoutMs() {
    return sendTimeoutMs;
  }

  public void setSendTimeoutMs(long sendTimeoutMs) {
    this.sendTimeoutMs = sendTimeoutMs;
  }

  public Backpressure getBackpressure() {
    return backpressure;
  }

  public void setBackpressure(Backpressure backpressure) {
    this.backpressure = backpressure;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public OutboundTopics toTopics() {
    Topics t = kafka.getTopics();
    return new OutboundTopics(
        t.getEventResult(), t.getTrade(), t.getOrderUpdate(), t.getBookUpdate());
  }

  public static class Backpressure {
    private double[] warnRatio = {0.5, 0.75, 0.9};
    private long logThrottleMs = 5_000L;

    public double[] getWarnRatio() {
      return warnRatio;
    }

    public void setWarnRatio(double[] warnRatio) {
      this.warnRatio = warnRatio;
    }

    public long getLogThrottleMs() {
      return logThrottleMs;
    }

    public void setLogThrottleMs(long logThrottleMs) {
      this.logThrottleMs = logThrottleMs;
    }
  }

  public static class Kafka {
    private String bootstrapServers = "localhost:9092";
    private Topics topics = new Topics();

    public String getBootstrapServers() {
      return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
      this.bootstrapServers = bootstrapServers;
    }

    public Topics getTopics() {
      return topics;
    }

    public void setTopics(Topics topics) {
      this.topics = topics;
    }
  }

  public static class Topics {
    private String eventResult = "event_result";
    private String trade = "trade";
    private String orderUpdate = "order_update";
    private String bookUpdate = "book_update";

    public String getEventResult() {
      return eventResult;
    }

    public void setEventResult(String eventResult) {
      this.eventResult = eventResult;
    }

    public String getTrade() {
      return trade;
    }

    public void setTrade(String trade) {
      this.trade = trade;
    }

    public String getOrderUpdate() {
      return orderUpdate;
    }

    public void setOrderUpdate(String orderUpdate) {
      this.orderUpdate = orderUpdate;
    }

    public String getBookUpdate() {
      return bookUpdate;
    }

    public void setBookUpdate(String bookUpdate) {
      this.bookUpdate = bookUpdate;
    }
  }
}
