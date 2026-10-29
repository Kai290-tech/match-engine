package com.kai.matchengine.ingress;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 单线程 Kafka 入站：{@code subscribe} 进 consumer group → 分配后 {@code seek(快照 offset)} → poll。
 *
 * <p>{@code groupId} 应由调用方在<strong>每次进程启动</strong>时生成新的 UUID，避免沿用旧 group 的
 * committed offset（该位点可能超前于撮合/快照）。消费起点以 {@link OffsetStore} seek 为准。
 *
 * <p>非法消息不进撮合，但仍 {@code commitSync}；该 commit 不是撮合/快照水位。
 */
public final class KafkaIngressRunner implements Runnable, AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(KafkaIngressRunner.class);

  private final String bootstrapServers;
  private final String topic;
  private final int partition;
  private final String groupId;
  private final OffsetStore offsetStore;
  private final MatchEventProcessor processor;
  private final Duration pollTimeout;

  private final AtomicBoolean running = new AtomicBoolean(false);
  private volatile KafkaConsumer<byte[], byte[]> consumer;

  public KafkaIngressRunner(
      String bootstrapServers,
      String topic,
      int partition,
      String groupId,
      OffsetStore offsetStore,
      MatchEventProcessor processor,
      Duration pollTimeout) {
    this.bootstrapServers = Objects.requireNonNull(bootstrapServers);
    this.topic = Objects.requireNonNull(topic);
    this.partition = partition;
    this.groupId = Objects.requireNonNull(groupId);
    this.offsetStore = Objects.requireNonNull(offsetStore);
    this.processor = Objects.requireNonNull(processor);
    this.pollTimeout = Objects.requireNonNull(pollTimeout);
  }

  @Override
  public void run() {
    if (!running.compareAndSet(false, true)) {
      return;
    }
    Properties props = consumerProps();
    KafkaConsumer<byte[], byte[]> local = new KafkaConsumer<>(props);
    this.consumer = local;
    try {
      local.subscribe(
          List.of(topic),
          new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
              log.info("Kafka ingress partitions revoked {}", partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
              Map<TopicPartition, Long> seeks = new HashMap<>();
              for (TopicPartition tp : partitions) {
                if (tp.partition() != partition) {
                  log.warn(
                      "assigned unexpected partition {} (configured {}); still seeking from store",
                      tp.partition(),
                      partition);
                }
                long start = offsetStore.loadStartingOffset(tp.topic(), tp.partition());
                seeks.put(tp, start);
                local.seek(tp, start);
              }
              log.info(
                  "Kafka ingress partitions assigned groupId={} seeks={}", groupId, seeks);
            }
          });
      log.info(
          "Kafka ingress started subscribe topic={} preferPartition={} groupId={}",
          topic,
          partition,
          groupId);
      while (running.get()) {
        ConsumerRecords<byte[], byte[]> records = local.poll(pollTimeout);
        if (records.isEmpty()) {
          continue;
        }
        for (ConsumerRecord<byte[], byte[]> record : records) {
          processor.process(record.value(), record.topic(), record.partition(), record.offset());
        }
        // commit = ingress 进度，不是撮合/快照水位；恢复仍以快照 seek 为准
        local.commitSync();
      }
    } catch (org.apache.kafka.common.errors.WakeupException wake) {
      if (running.get()) {
        throw wake;
      }
    } finally {
      closeConsumer(local);
      this.consumer = null;
      running.set(false);
      log.info("Kafka ingress stopped");
    }
  }

  public void stop() {
    running.set(false);
    KafkaConsumer<byte[], byte[]> c = consumer;
    if (c != null) {
      c.wakeup();
    }
  }

  @Override
  public void close() {
    stop();
  }

  private void closeConsumer(KafkaConsumer<byte[], byte[]> c) {
    if (c == null) {
      return;
    }
    try {
      c.close(Duration.ofSeconds(5));
    } catch (Exception e) {
      log.warn("close consumer error", e);
    }
  }

  private Properties consumerProps() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return props;
  }
}
