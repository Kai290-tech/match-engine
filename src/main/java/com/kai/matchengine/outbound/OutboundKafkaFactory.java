package com.kai.matchengine.outbound;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * 创建出站 {@link KafkaProducer}：按事件类型发到 {@link OutboundTopics} 中配置的 topic。
 *
 * <p>实际 {@code producer.send(new ProducerRecord(topic, ...))} 见 {@link KafkaAsyncRecordSender}；
 * topic 选择见 {@link OutboundJsonEncoder#topicFor}。
 */
public final class OutboundKafkaFactory {

  private OutboundKafkaFactory() {}

  public static KafkaProducer<byte[], byte[]> createProducer(String bootstrapServers) {
    return createProducer(bootstrapServers, Map.of());
  }

  public static KafkaProducer<byte[], byte[]> createProducer(
      String bootstrapServers, Map<String, String> extra) {
    Objects.requireNonNull(bootstrapServers, "bootstrapServers");
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    props.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
    props.put(ProducerConfig.LINGER_MS_CONFIG, "0");
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "match-engine-outbound");
    if (extra != null) {
      props.putAll(extra);
    }
    return new KafkaProducer<>(props);
  }

  public static OutboundTopics topicsFrom(
      String eventResult, String trade, String orderUpdate, String bookUpdate) {
    return new OutboundTopics(eventResult, trade, orderUpdate, bookUpdate);
  }

  /** 组装：KafkaProducer → AsyncSender → Publisher（使用给定出站 topic 名）。 */
  public static OutboundPublisher createPublisher(
      String bootstrapServers,
      OutboundTopics topics,
      OutboundWatermark watermark,
      com.kai.matchengine.ingress.FatalFailureHandler fatal,
      int queueCapacity,
      long sendTimeoutMs) {
    KafkaProducer<byte[], byte[]> producer = createProducer(bootstrapServers);
    AsyncRecordSender sender = new KafkaAsyncRecordSender(producer);
    return new OutboundPublisher(
        queueCapacity,
        watermark,
        sender,
        topics,
        new SbeOutboundEncoder(),
        fatal,
        sendTimeoutMs);
  }

  public static Map<String, String> defaultExtraProps() {
    return new HashMap<>();
  }
}
