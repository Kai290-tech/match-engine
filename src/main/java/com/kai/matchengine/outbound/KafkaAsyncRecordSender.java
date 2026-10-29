package com.kai.matchengine.outbound;

import java.util.Objects;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * 基于 Kafka {@link Producer} 的异步发送。
 *
 * <p>{@code topic} 由 {@link OutboundPublisher} 按事件类型从 {@link OutboundTopics} 选出，再 {@code
 * producer.send(new ProducerRecord(topic, key, value), callback)}。
 */
public final class KafkaAsyncRecordSender implements AsyncRecordSender, AutoCloseable {

  private final Producer<byte[], byte[]> producer;

  public KafkaAsyncRecordSender(Producer<byte[], byte[]> producer) {
    this.producer = Objects.requireNonNull(producer, "producer");
  }

  @Override
  public void sendAsync(String topic, byte[] key, byte[] value, Completion completion) {
    producer.send(
        new ProducerRecord<>(topic, key, value),
        (metadata, exception) -> {
          if (exception != null) {
            completion.onComplete(false, exception);
          } else {
            completion.onComplete(true, null);
          }
        });
  }

  @Override
  public void close() {
    producer.close();
  }
}
