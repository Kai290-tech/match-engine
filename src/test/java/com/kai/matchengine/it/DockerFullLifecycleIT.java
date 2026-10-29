package com.kai.matchengine.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.kai.matchengine.event.inbound.CancelOrder;
import com.kai.matchengine.event.inbound.CreateSymbol;
import com.kai.matchengine.event.inbound.InboundEventType;
import com.kai.matchengine.event.inbound.OrderType;
import com.kai.matchengine.event.inbound.PlaceOrder;
import com.kai.matchengine.event.inbound.UpdateSymbol;
import com.kai.matchengine.ingress.FatalFailureHandler;
import com.kai.matchengine.ingress.IngressValidator;
import com.kai.matchengine.ingress.KafkaIngressRunner;
import com.kai.matchengine.ingress.MatchEventProcessor;
import com.kai.matchengine.ingress.OffsetStore;
import com.kai.matchengine.ingress.SbeMatchEventCodec;
import com.kai.matchengine.ingress.SbeMatchEventEncoder;
import com.kai.matchengine.ingress.disruptor.MatchIngressDisruptor;
import com.kai.matchengine.match.EngineRegistry;
import com.kai.matchengine.match.SystemTimeSource;
import com.kai.matchengine.orderbook.Side;
import com.kai.matchengine.outbound.KafkaAsyncRecordSender;
import com.kai.matchengine.outbound.MatchPipelineOutboundListener;
import com.kai.matchengine.outbound.OutboundBatch;
import com.kai.matchengine.outbound.OutboundKafkaFactory;
import com.kai.matchengine.outbound.OutboundPublisher;
import com.kai.matchengine.outbound.OutboundTopics;
import com.kai.matchengine.outbound.OutboundWatermark;
import com.kai.matchengine.outbound.SbeOutboundDecoder;
import com.kai.matchengine.outbound.SbeOutboundEncoder;
import com.kai.matchengine.snapshot.JdbcSnapshotStore;
import com.kai.matchengine.snapshot.SnapshotCoordinator;
import com.kai.matchengine.snapshot.delta.BookDeltaLog;
import com.kai.matchengine.snapshot.delta.BookDeltaRecorder;
import com.kai.matchengine.snapshot.delta.JdbcBookDeltaBatchStore;
import com.kai.matchengine.snapshot.delta.SnapshotMaterializer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 对接本机 Docker（MySQL + Kafka）的完整生命周期：
 *
 * <pre>
 * CreateSymbol → UpdateSymbol → Place → Cancel → Place(maker) → Place(taker成交)
 * </pre>
 *
 * <p>链路：Kafka match_event → Ingress → Disruptor → Outbound → event_result/trade，快照走 JDBC。
 *
 * <p>运行前：{@code docker compose up -d}。无 Docker 时自动 skip。
 *
 * <pre>
 * ./mvnw -Dtest=DockerFullLifecycleIT test
 * </pre>
 */
@Tag("docker")
class DockerFullLifecycleIT {

  private static final String BOOTSTRAP = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");
  private static final String MYSQL_URL =
      System.getenv()
          .getOrDefault(
              "MYSQL_URL",
              "jdbc:mysql://localhost:3306/match_engine?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC");
  private static final String MYSQL_USER = System.getenv().getOrDefault("MYSQL_USER", "root");
  private static final String MYSQL_PASSWORD = System.getenv().getOrDefault("MYSQL_PASSWORD", "root");

  private static final String MATCH_TOPIC = "match_event";
  private static final int MATCH_PARTITION = 0;
  private static final OutboundTopics TOPICS = OutboundTopics.DEFAULTS;

  private HikariDataSource dataSource;
  private MatchIngressDisruptor disruptor;
  private OutboundPublisher publisher;
  private SnapshotCoordinator snapshotCoordinator;
  private KafkaIngressRunner ingressRunner;
  private Thread ingressThread;
  private KafkaProducer<byte[], byte[]> ingressProducer;
  private KafkaConsumer<byte[], byte[]> resultConsumer;
  private KafkaProducer<byte[], byte[]> outboundProducer;

  @AfterEach
  void tearDown() {
    if (ingressRunner != null) {
      ingressRunner.close();
    }
    if (ingressThread != null) {
      try {
        ingressThread.join(5_000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    if (disruptor != null) {
      disruptor.close();
    }
    if (publisher != null) {
      publisher.close();
    }
    if (snapshotCoordinator != null) {
      snapshotCoordinator.close();
    }
    if (ingressProducer != null) {
      ingressProducer.close();
    }
    if (outboundProducer != null) {
      outboundProducer.close();
    }
    if (resultConsumer != null) {
      resultConsumer.close();
    }
    if (dataSource != null) {
      dataSource.close();
    }
  }

  @Test
  void lifecycle_create_update_place_cancel_place_trade() throws Exception {
    assumeTrue(portOpen("localhost", 9092), "Kafka not reachable on :9092 — start docker compose");
    assumeTrue(portOpen("localhost", 3306), "MySQL not reachable on :3306 — start docker compose");

    AtomicReference<Throwable> fatal = new AtomicReference<>();
    FatalFailureHandler fatalHandler =
        (msg, cause) -> {
          fatal.set(cause != null ? cause : new IllegalStateException(msg));
        };

    dataSource = mysqlDataSource();
    JdbcSnapshotStore snapshotStore = new JdbcSnapshotStore(dataSource);
    snapshotStore.initSchema();
    JdbcBookDeltaBatchStore batchStore = new JdbcBookDeltaBatchStore(dataSource);
    batchStore.initSchema();
    BookDeltaLog deltaLog = new BookDeltaLog();
    BookDeltaRecorder deltaRecorder = new BookDeltaRecorder(deltaLog);
    SnapshotMaterializer materializer = new SnapshotMaterializer();

    EngineRegistry registry =
        new EngineRegistry(SystemTimeSource.INSTANCE, 300_000, deltaRecorder);
    OutboundWatermark watermark = new OutboundWatermark();

    outboundProducer = OutboundKafkaFactory.createProducer(BOOTSTRAP);
    publisher =
        new OutboundPublisher(
            4096,
            watermark,
            new KafkaAsyncRecordSender(outboundProducer),
            TOPICS,
            new SbeOutboundEncoder(),
            fatalHandler,
            30_000);

    snapshotCoordinator =
        new SnapshotCoordinator(
            registry,
            snapshotStore,
            batchStore,
            deltaLog,
            materializer,
            watermark,
            fatalHandler,
            MATCH_TOPIC,
            MATCH_PARTITION,
            10_000,
            10_000,
            0.30,
            2,
            30_000);
    snapshotCoordinator.bootstrapFromStore();

    disruptor =
        new MatchIngressDisruptor(
            registry,
            1024 * 16,
            MatchIngressDisruptor.WaitMode.BLOCKING,
            null,
            new MatchPipelineOutboundListener(
                publisher, snapshotCoordinator, MATCH_TOPIC, MATCH_PARTITION, System::currentTimeMillis),
            fatalHandler);

    long startOffset = endOffset(MATCH_TOPIC, MATCH_PARTITION);
    OffsetStore offsetStore = (topic, partition) -> startOffset;
    watermark.seedBefore(startOffset);

    MatchEventProcessor processor =
        new MatchEventProcessor(
            new SbeMatchEventCodec(),
            new IngressValidator(),
            disruptor.publisher(),
            fatalHandler,
            (topic, partition, offset) ->
                publisher.enqueueUninterruptibly(
                    new OutboundBatch(topic, partition, offset, 0, List.of())));

    String groupId = "match-engine-it-" + UUID.randomUUID();
    ingressRunner =
        new KafkaIngressRunner(
            BOOTSTRAP,
            MATCH_TOPIC,
            MATCH_PARTITION,
            groupId,
            offsetStore,
            processor,
            Duration.ofMillis(200));

    snapshotCoordinator.start();
    publisher.start();
    disruptor.start();
    ingressThread = new Thread(ingressRunner, "it-kafka-ingress");
    ingressThread.start();

    // 出站消费者：从当前 end 开始，只收本轮产出
    resultConsumer = outboundConsumer();
    Map<TopicPartition, Long> outboundEnds = new HashMap<>();
    for (String t : List.of(TOPICS.eventResult(), TOPICS.trade())) {
      TopicPartition tp = new TopicPartition(t, 0);
      outboundEnds.put(tp, endOffset(t, 0));
    }
    resultConsumer.assign(new ArrayList<>(outboundEnds.keySet()));
    for (var e : outboundEnds.entrySet()) {
      resultConsumer.seek(e.getKey(), e.getValue());
    }

    ingressProducer = ingressProducer();
    SbeMatchEventEncoder encoder = new SbeMatchEventEncoder();

    int symbolId = (int) (System.currentTimeMillis() % 1_000_000_000L);
    if (symbolId <= 0) {
      symbolId = 1;
    }
    long req = 1;
    long orderCancel = 1001;
    long orderMaker = 1002;
    long orderTaker = 1003;
    final int expectedSymbolId = symbolId;

    // 1) CreateSymbol
    send(encoder.encode(new CreateSymbol(symbolId, req++, "ETHUSDT")));
    // 2) UpdateSymbol（code 不可变；同 code 幂等）
    send(encoder.encode(new UpdateSymbol(symbolId, req++, "ETHUSDT")));
    // 3) Place → 挂单
    send(
        encoder.encode(
            new PlaceOrder(
                symbolId, req++, orderCancel, 1, Side.BID, OrderType.LIMIT, 100, 5)));
    // 4) Cancel
    send(encoder.encode(new CancelOrder(symbolId, req++, orderCancel, 1)));
    // 5) Place maker
    send(
        encoder.encode(
            new PlaceOrder(
                symbolId, req++, orderMaker, 1, Side.BID, OrderType.LIMIT, 100, 5)));
    // 6) Place taker → 成交
    send(
        encoder.encode(
            new PlaceOrder(
                symbolId, req++, orderTaker, 2, Side.ASK, OrderType.LIMIT, 100, 5)));

    List<byte[]> results = new CopyOnWriteArrayList<>();
    List<byte[]> trades = new CopyOnWriteArrayList<>();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (System.nanoTime() < deadline) {
      assertEquals(null, fatal.get(), () -> "fatal: " + fatal.get());
      ConsumerRecords<byte[], byte[]> polled = resultConsumer.poll(Duration.ofMillis(200));
      for (ConsumerRecord<byte[], byte[]> r : polled) {
        if (TOPICS.eventResult().equals(r.topic())) {
          results.add(r.value());
        } else if (TOPICS.trade().equals(r.topic())) {
          trades.add(r.value());
        }
      }
      if (countAccepted(results, InboundEventType.CREATE_SYMBOL) >= 1
          && countAccepted(results, InboundEventType.UPDATE_SYMBOL) >= 1
          && countAcceptedPlace(results) >= 3
          && countAcceptedCancel(results) >= 1
          && !trades.isEmpty()) {
        break;
      }
    }

    assertEquals(null, fatal.get());
    assertTrue(
        countAccepted(results, InboundEventType.CREATE_SYMBOL) >= 1,
        "missing CREATE_SYMBOL ok: " + results.size());
    assertTrue(
        countAccepted(results, InboundEventType.UPDATE_SYMBOL) >= 1,
        "missing UPDATE_SYMBOL ok: " + results.size());
    assertTrue(countAcceptedCancel(results) >= 1, "missing CANCEL ok: " + results.size());
    assertTrue(countAcceptedPlace(results) >= 3, "need 3 PLACE results: " + results.size());
    assertTrue(
        trades.stream().anyMatch(
            t -> SbeOutboundDecoder.isTrade(t)
                && SbeOutboundDecoder.tradeSymbolId(t) == expectedSymbolId),
        "missing TRADE for symbolId=" + expectedSymbolId);

    assertEquals("ETHUSDT", registry.symbols().get(symbolId).code());
    assertTrue(registry.get(symbolId).book().isEmpty(), "full fill should clear book");
    assertTrue(watermark.ackedOffset() >= startOffset + 5, "watermark should cover lifecycle offsets");
  }

  private void send(byte[] payload) throws Exception {
    ingressProducer
        .send(new ProducerRecord<>(MATCH_TOPIC, MATCH_PARTITION, null, payload))
        .get(10, TimeUnit.SECONDS);
  }

  private static int countAccepted(List<byte[]> results, InboundEventType type) {
    int n = 0;
    for (byte[] payload : results) {
      if (SbeOutboundDecoder.isAcceptedSymbolEvent(payload, type)) {
        n++;
      }
    }
    return n;
  }

  private static int countAcceptedPlace(List<byte[]> results) {
    int n = 0;
    for (byte[] payload : results) {
      if (SbeOutboundDecoder.isAcceptedPlace(payload)) {
        n++;
      }
    }
    return n;
  }

  private static int countAcceptedCancel(List<byte[]> results) {
    int n = 0;
    for (byte[] payload : results) {
      if (SbeOutboundDecoder.isAcceptedCancel(payload)) {
        n++;
      }
    }
    return n;
  }

  private static boolean portOpen(String host, int port) {
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress(host, port), 500);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private static HikariDataSource mysqlDataSource() {
    HikariConfig cfg = new HikariConfig();
    cfg.setJdbcUrl(MYSQL_URL);
    cfg.setUsername(MYSQL_USER);
    cfg.setPassword(MYSQL_PASSWORD);
    cfg.setMaximumPoolSize(2);
    return new HikariDataSource(cfg);
  }

  private static long endOffset(String topic, int partition) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-end-" + UUID.randomUUID());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(props)) {
      TopicPartition tp = new TopicPartition(topic, partition);
      c.assign(List.of(tp));
      return c.endOffsets(List.of(tp)).get(tp);
    }
  }

  private static KafkaProducer<byte[], byte[]> ingressProducer() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "docker-lifecycle-it-producer");
    return new KafkaProducer<>(props);
  }

  private static KafkaConsumer<byte[], byte[]> outboundConsumer() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "docker-lifecycle-it-" + UUID.randomUUID());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    return new KafkaConsumer<>(props);
  }
}
