package com.kai.matchengine.outbound;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kai.matchengine.event.outbound.BookUpdate;
import com.kai.matchengine.event.outbound.CommandResult;
import com.kai.matchengine.event.outbound.OrderUpdate;
import com.kai.matchengine.event.outbound.OrderUpdateType;
import com.kai.matchengine.event.outbound.Trade;
import com.kai.matchengine.orderbook.Side;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** 验证 Kafka ProducerRecord 打到配置的出站 topic。 */
class OutboundKafkaTopicRoutingTest {

  private OutboundPublisher publisher;
  private MockProducer<byte[], byte[]> mock;

  @AfterEach
  void tearDown() {
    if (publisher != null) {
      publisher.close();
    }
    if (mock != null) {
      mock.close();
    }
  }

  private static MockProducer<byte[], byte[]> newAutoCompleteMock() {
    Partitioner partitioner =
        new Partitioner() {
          @Override
          public int partition(
              String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
            return 0;
          }

          @Override
          public void close() {}

          @Override
          public void configure(Map<String, ?> configs) {}
        };
    return new MockProducer<>(true, partitioner, new ByteArraySerializer(), new ByteArraySerializer());
  }

  @Test
  void publisher_sendsEachEventType_toConfiguredTopics() throws Exception {
    mock = newAutoCompleteMock();
    OutboundTopics topics =
        new OutboundTopics("event_result", "trade", "order_update", "book_update");
    OutboundWatermark watermark = new OutboundWatermark();
    AtomicReference<Throwable> fatal = new AtomicReference<>();

    publisher =
        new OutboundPublisher(
            16,
            watermark,
            new KafkaAsyncRecordSender(mock),
            topics,
            new SbeOutboundEncoder(),
            (m, c) -> fatal.set(c),
            5_000);
    publisher.start();

    publisher.enqueue(
        new OutboundBatch(
            "match_event",
            0,
            0L,
            1,
            List.of(
                CommandResult.Place.ok(99L, 1L, "ETHUSDT", 1L, 0L, 1L, true),
                new Trade(1, 1, "ETHUSDT", 1, 1, 2, 2, 100, 1, Side.BID),
                new OrderUpdate(
                    2, 1, "ETHUSDT", 1, 1, Side.BID, 100, 0, 1, OrderUpdateType.FILLED),
                new BookUpdate(1, 1, "ETHUSDT", Side.BID, 100, 0))));

    assertTrue(watermark.awaitAtLeast(0, 3, TimeUnit.SECONDS));
    assertEquals(null, fatal.get());

    Map<String, Long> byTopic =
        mock.history().stream()
            .collect(Collectors.groupingBy(ProducerRecord::topic, Collectors.counting()));

    assertEquals(1L, byTopic.get("event_result"));
    assertEquals(1L, byTopic.get("trade"));
    assertEquals(1L, byTopic.get("order_update"));
    assertEquals(1L, byTopic.get("book_update"));

    for (ProducerRecord<byte[], byte[]> r : mock.history()) {
      assertEquals("1", new String(r.key(), StandardCharsets.UTF_8));
      assertTrue(r.value().length > 0);
    }
  }

  @Test
  void kafkaKey_usesSymbolId() {
    OutboundBatch batch = new OutboundBatch("match_event", 0, 9L, 42, List.of());
    assertEquals(
        "42",
        new String(
            OutboundPublisher.kafkaKey(
                batch, new Trade(1, 1, "ETHUSDT", 1, 1, 2, 2, 100, 1, Side.BID)),
            StandardCharsets.UTF_8));
  }

  @Test
  void customTopicNames_areUsedInProducerRecord() throws Exception {
    mock = newAutoCompleteMock();
    OutboundTopics topics = OutboundKafkaFactory.topicsFrom("er", "tr", "ou", "bu");
    OutboundWatermark watermark = new OutboundWatermark();

    publisher =
        new OutboundPublisher(
            8,
            watermark,
            new KafkaAsyncRecordSender(mock),
            topics,
            new SbeOutboundEncoder(),
            (m, c) -> {},
            3_000);
    publisher.start();

    publisher.enqueue(
        new OutboundBatch(
            "match_event",
            0,
            0L,
            1,
            List.of(CommandResult.Cancel.ok(1L, 1L, "S-1", 1L))));

    assertTrue(watermark.awaitAtLeast(0, 2, TimeUnit.SECONDS));
    assertEquals("er", mock.history().getFirst().topic());
    assertEquals("1", new String(mock.history().getFirst().key(), StandardCharsets.UTF_8));
  }
}
