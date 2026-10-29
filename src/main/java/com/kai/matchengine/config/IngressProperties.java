package com.kai.matchengine.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ingress")
public class IngressProperties {

  private Kafka kafka = new Kafka();
  private String symbols = "1";

  public Kafka getKafka() {
    return kafka;
  }

  public void setKafka(Kafka kafka) {
    this.kafka = kafka;
  }

  public String getSymbols() {
    return symbols;
  }

  public void setSymbols(String symbols) {
    this.symbols = symbols;
  }

  public int[] symbolIds() {
    if (symbols == null || symbols.isBlank()) {
      return new int[0];
    }
    String[] parts = symbols.split(",");
    int[] ids = new int[parts.length];
    int n = 0;
    for (String part : parts) {
      String t = part.trim();
      if (t.isEmpty()) {
        continue;
      }
      ids[n++] = Integer.parseInt(t);
    }
    if (n == ids.length) {
      return ids;
    }
    int[] trimmed = new int[n];
    System.arraycopy(ids, 0, trimmed, 0, n);
    return trimmed;
  }

  public static class Kafka {
    private String bootstrapServers = "localhost:9092";
    private String topic = "match_event";
    private int partition = 0;
    private String groupId = "match-engine";
    private long startOffset = 0L;

    public String getBootstrapServers() {
      return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
      this.bootstrapServers = bootstrapServers;
    }

    public String getTopic() {
      return topic;
    }

    public void setTopic(String topic) {
      this.topic = topic;
    }

    public int getPartition() {
      return partition;
    }

    public void setPartition(int partition) {
      this.partition = partition;
    }

    public String getGroupId() {
      return groupId;
    }

    public void setGroupId(String groupId) {
      this.groupId = groupId;
    }

    public long getStartOffset() {
      return startOffset;
    }

    public void setStartOffset(long startOffset) {
      this.startOffset = startOffset;
    }
  }
}
