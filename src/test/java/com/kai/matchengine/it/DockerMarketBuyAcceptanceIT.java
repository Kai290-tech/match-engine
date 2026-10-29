package com.kai.matchengine.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.kai.matchengine.event.inbound.CreateSymbol;
import com.kai.matchengine.event.inbound.OrderType;
import com.kai.matchengine.event.inbound.PlaceOrder;
import com.kai.matchengine.event.inbound.QtyUnit;
import com.kai.matchengine.ingress.FatalFailureHandler;
import com.kai.matchengine.ingress.IngressValidator;
import com.kai.matchengine.ingress.KafkaIngressRunner;
import com.kai.matchengine.ingress.MatchEventProcessor;
import com.kai.matchengine.ingress.OffsetStore;
import com.kai.matchengine.ingress.SbeMatchEventCodec;
import com.kai.matchengine.ingress.SbeMatchEventEncoder;
import com.kai.matchengine.ingress.disruptor.MatchIngressDisruptor;
import com.kai.matchengine.match.EngineRegistry;
import com.kai.matchengine.match.MatchingEngine;
import com.kai.matchengine.match.SystemTimeSource;
import com.kai.matchengine.orderbook.Side;
import com.kai.matchengine.outbound.KafkaAsyncRecordSender;
import com.kai.matchengine.outbound.MatchPipelineOutboundListener;
import com.kai.matchengine.outbound.OutboundBatch;
import com.kai.matchengine.outbound.OutboundKafkaFactory;
import com.kai.matchengine.outbound.OutboundPublisher;
import com.kai.matchengine.outbound.OutboundTopics;
import com.kai.matchengine.outbound.OutboundWatermark;
import com.kai.matchengine.outbound.SbeOutboundEncoder;
import com.kai.matchengine.snapshot.JdbcSnapshotStore;
import com.kai.matchengine.snapshot.SnapshotCoordinator;
import com.kai.matchengine.snapshot.delta.BookDeltaLog;
import com.kai.matchengine.snapshot.delta.BookDeltaRecorder;
import com.kai.matchengine.snapshot.delta.JdbcBookDeltaBatchStore;
import com.kai.matchengine.snapshot.delta.SnapshotMaterializer;
import com.kai.matchengine.sbe.outbound.CommandResultPlaceDecoder;
import com.kai.matchengine.sbe.outbound.MessageHeaderDecoder;
import com.kai.matchengine.sbe.outbound.OrderUpdateDecoder;
import com.kai.matchengine.sbe.outbound.TradeDecoder;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.agrona.concurrent.UnsafeBuffer;
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
 * 测试专家验收节点 {@code ME-MKT-BUY-001-T}。
 *
 * <p>经本机 Kafka {@code match_event} 进入撮合，再从 {@code event_result}、{@code trade}、
 * {@code order_update} 读出站。市价买单按计价预算向下取整，成交额不超过预算。
 *
 * <p>运行前：{@code docker compose up -d}。
 */
@Tag("docker")
class DockerMarketBuyAcceptanceIT {

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
  private static final int POOL_CAPACITY = 8;

  private HikariDataSource dataSource;
  private MatchIngressDisruptor disruptor;
  private OutboundPublisher publisher;
  private SnapshotCoordinator snapshotCoordinator;
  private KafkaIngressRunner ingressRunner;
  private Thread ingressThread;
  private KafkaProducer<byte[], byte[]> ingressProducer;
  private KafkaConsumer<byte[], byte[]> outboundConsumer;
  private KafkaProducer<byte[], byte[]> outboundProducer;

  private final List<byte[]> results = new ArrayList<>();
  private final List<byte[]> trades = new ArrayList<>();
  private final List<byte[]> orderUpdates = new ArrayList<>();

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
    if (outboundConsumer != null) {
      outboundConsumer.close();
    }
    if (dataSource != null) {
      dataSource.close();
    }
  }

  @Test
  void meMktBuy001T_f23_f24_overKafka() throws Exception {
    assumeTrue(portOpen("localhost", 9092), "Kafka not reachable on :9092 — start docker compose");
    assumeTrue(portOpen("localhost", 3306), "MySQL not reachable on :3306 — start docker compose");

    AtomicReference<Throwable> fatal = new AtomicReference<>();
    EngineRegistry registry = startPipeline(fatal);
    SbeMatchEventEncoder encoder = new SbeMatchEventEncoder();
    int symbolId = symbolId();
    long req = 1;

    send(encoder.encode(new CreateSymbol(symbolId, req++, "MKTBUY")));
    await(fatal, () -> registry.contains(symbolId));

    long askNear = 501;
    long askFar = 502;
    send(encoder.encode(new PlaceOrder(symbolId, req++, askNear, 1, Side.ASK, OrderType.LIMIT, 100, 2)));
    send(encoder.encode(new PlaceOrder(symbolId, req++, askFar, 1, Side.ASK, OrderType.LIMIT, 150, 2)));
    await(fatal, () -> book(registry, symbolId).contains(askNear) && book(registry, symbolId).contains(askFar));

    long tooSmall = 601;
    send(encoder.encode(new PlaceOrder(symbolId, req++, tooSmall, 2, Side.BID, OrderType.MARKET, 0, 99, QtyUnit.QUOTE)));
    await(fatal, () -> place(tooSmall) != null && cancelledRemainder(tooSmall));
    PlaceView killed = place(tooSmall);
    assertTrue(killed.accepted(), killed.toString());
    assertEquals(0L, killed.filledQty());
    assertEquals(0L, killed.remainingQty());
    assertFalse(killed.resting());
    assertFalse(tradeForTaker(tooSmall), "F24 must not trade");
    assertEquals(2L, book(registry, symbolId).get(askNear).qty());
    assertEquals(2L, book(registry, symbolId).get(askFar).qty());

    send(encoder.encode(new PlaceOrder(symbolId, req++, tooSmall, 2, Side.BID, OrderType.MARKET, 0, 450, QtyUnit.QUOTE)));
    await(fatal, () -> rejectedDuplicate(tooSmall));
    assertEquals(2L, book(registry, symbolId).get(askNear).qty());

    long budgetBuy = 701;
    send(encoder.encode(new PlaceOrder(symbolId, req++, budgetBuy, 2, Side.BID, OrderType.MARKET, 0, 450, QtyUnit.QUOTE)));
    await(
        fatal,
        () ->
            place(budgetBuy) != null
                && tradeMakerQty(budgetBuy, askNear, 100, 2)
                && tradeMakerQty(budgetBuy, askFar, 150, 1)
                && cancelledRemainder(budgetBuy));
    PlaceView bought = place(budgetBuy);
    assertTrue(bought.accepted(), bought.toString());
    assertEquals(3L, bought.filledQty());
    assertEquals(0L, bought.remainingQty());
    assertFalse(bought.resting());
    assertTrue(notional(budgetBuy) <= 450L);
    assertEquals(350L, notional(budgetBuy));
    assertFalse(book(registry, symbolId).contains(askNear));
    assertEquals(1L, book(registry, symbolId).get(askFar).qty());
    assertFalse(book(registry, symbolId).contains(budgetBuy));

    long exact = 801;
    send(encoder.encode(new PlaceOrder(symbolId, req++, exact, 2, Side.BID, OrderType.MARKET, 0, 150, QtyUnit.QUOTE)));
    await(fatal, () -> place(exact) != null && tradeMakerQty(exact, askFar, 150, 1) && takerUpdate(exact, "FILLED"));
    PlaceView spent = place(exact);
    assertTrue(spent.accepted(), spent.toString());
    assertEquals(1L, spent.filledQty());
    assertEquals(150L, notional(exact));
    assertTrue(book(registry, symbolId).isEmpty());
    assertFalse(cancelledRemainder(exact));

    long askBase = 503;
    send(encoder.encode(new PlaceOrder(symbolId, req++, askBase, 1, Side.ASK, OrderType.LIMIT, 100, 2)));
    await(fatal, () -> book(registry, symbolId).contains(askBase));
    long baseBuy = 901;
    send(encoder.encode(new PlaceOrder(symbolId, req++, baseBuy, 2, Side.BID, OrderType.MARKET, 0, 5)));
    await(fatal, () -> place(baseBuy) != null && tradeMakerQty(baseBuy, askBase, 100, 2) && cancelledRemainder(baseBuy));
    PlaceView byQty = place(baseBuy);
    assertTrue(byQty.accepted(), byQty.toString());
    assertEquals(2L, byQty.filledQty());
    assertEquals(0L, byQty.remainingQty());
    assertFalse(byQty.resting());
    assertTrue(book(registry, symbolId).isEmpty());
    assertEquals(null, fatal.get());
  }

  private EngineRegistry startPipeline(AtomicReference<Throwable> fatal) throws Exception {
    FatalFailureHandler fatalHandler =
        (msg, cause) -> fatal.set(cause != null ? cause : new IllegalStateException(msg));
    dataSource = mysql();
    JdbcSnapshotStore snapshotStore = new JdbcSnapshotStore(dataSource);
    snapshotStore.initSchema();
    JdbcBookDeltaBatchStore batchStore = new JdbcBookDeltaBatchStore(dataSource);
    batchStore.initSchema();
    BookDeltaLog deltaLog = new BookDeltaLog();
    BookDeltaRecorder deltaRecorder = new BookDeltaRecorder(deltaLog);
    EngineRegistry registry = new EngineRegistry(SystemTimeSource.INSTANCE, POOL_CAPACITY, deltaRecorder);
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
            new SnapshotMaterializer(),
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
                publisher.enqueueUninterruptibly(new OutboundBatch(topic, partition, offset, 0, List.of())));
    ingressRunner =
        new KafkaIngressRunner(
            BOOTSTRAP,
            MATCH_TOPIC,
            MATCH_PARTITION,
            "match-engine-mktbuy-it-" + UUID.randomUUID(),
            offsetStore,
            processor,
            Duration.ofMillis(200));
    snapshotCoordinator.start();
    publisher.start();
    disruptor.start();
    ingressThread = new Thread(ingressRunner, "it-mktbuy-ingress");
    ingressThread.start();
    outboundConsumer = outboundConsumer();
    Map<TopicPartition, Long> ends = new HashMap<>();
    for (String topic : List.of(TOPICS.eventResult(), TOPICS.trade(), TOPICS.orderUpdate())) {
      TopicPartition tp = new TopicPartition(topic, 0);
      ends.put(tp, endOffset(topic, 0));
    }
    outboundConsumer.assign(new ArrayList<>(ends.keySet()));
    for (var e : ends.entrySet()) {
      outboundConsumer.seek(e.getKey(), e.getValue());
    }
    ingressProducer = ingressProducer();
    return registry;
  }

  private void await(AtomicReference<Throwable> fatal, BooleanSupplier ready) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
    while (System.nanoTime() < deadline) {
      assertEquals(null, fatal.get(), () -> String.valueOf(fatal.get()));
      poll();
      if (ready.getAsBoolean()) {
        return;
      }
    }
    assertTrue(ready.getAsBoolean(), "timeout results=" + results.size() + " trades=" + trades.size());
  }

  private void poll() {
    ConsumerRecords<byte[], byte[]> polled = outboundConsumer.poll(Duration.ofMillis(200));
    for (ConsumerRecord<byte[], byte[]> record : polled) {
      if (TOPICS.eventResult().equals(record.topic())) {
        results.add(record.value());
      } else if (TOPICS.trade().equals(record.topic())) {
        trades.add(record.value());
      } else if (TOPICS.orderUpdate().equals(record.topic())) {
        orderUpdates.add(record.value());
      }
    }
  }

  private void send(byte[] payload) throws Exception {
    ingressProducer.send(new ProducerRecord<>(MATCH_TOPIC, MATCH_PARTITION, null, payload)).get(10, TimeUnit.SECONDS);
  }

  private PlaceView place(long orderId) {
    PlaceView found = null;
    for (byte[] payload : results) {
      if (templateId(payload) != CommandResultPlaceDecoder.TEMPLATE_ID) {
        continue;
      }
      CommandResultPlaceDecoder decoder = new CommandResultPlaceDecoder();
      decoder.wrapAndApplyHeader(new UnsafeBuffer(payload), 0, new MessageHeaderDecoder());
      if (decoder.orderId() != orderId) {
        continue;
      }
      found =
          new PlaceView(
              decoder.orderId(),
              decoder.accepted() == 1,
              decoder.filledQty(),
              decoder.remainingQty(),
              decoder.resting() == 1,
              decoder.rejectReason().name());
    }
    return found;
  }

  private boolean rejectedDuplicate(long orderId) {
    PlaceView view = place(orderId);
    return view != null && !view.accepted() && "DUPLICATE_ORDER_ID".equals(view.rejectReason());
  }

  private boolean takerUpdate(long orderId, String type) {
    for (byte[] payload : orderUpdates) {
      if (templateId(payload) != OrderUpdateDecoder.TEMPLATE_ID) {
        continue;
      }
      OrderUpdateDecoder decoder = new OrderUpdateDecoder();
      decoder.wrapAndApplyHeader(new UnsafeBuffer(payload), 0, new MessageHeaderDecoder());
      if (decoder.orderId() == orderId && type.equals(decoder.updateType().name())) {
        return true;
      }
    }
    return false;
  }

  private boolean cancelledRemainder(long orderId) {
    for (byte[] payload : orderUpdates) {
      if (templateId(payload) != OrderUpdateDecoder.TEMPLATE_ID) {
        continue;
      }
      OrderUpdateDecoder decoder = new OrderUpdateDecoder();
      decoder.wrapAndApplyHeader(new UnsafeBuffer(payload), 0, new MessageHeaderDecoder());
      if (decoder.orderId() == orderId && "CANCELLED_REMAINDER".equals(decoder.updateType().name())) {
        return true;
      }
    }
    return false;
  }

  private boolean tradeForTaker(long takerOrderId) {
    for (byte[] payload : trades) {
      if (templateId(payload) != TradeDecoder.TEMPLATE_ID) {
        continue;
      }
      TradeDecoder decoder = new TradeDecoder();
      decoder.wrapAndApplyHeader(new UnsafeBuffer(payload), 0, new MessageHeaderDecoder());
      if (decoder.takerOrderId() == takerOrderId) {
        return true;
      }
    }
    return false;
  }

  private boolean tradeMakerQty(long takerOrderId, long makerOrderId, long priceTick, long qty) {
    for (byte[] payload : trades) {
      if (templateId(payload) != TradeDecoder.TEMPLATE_ID) {
        continue;
      }
      TradeDecoder decoder = new TradeDecoder();
      decoder.wrapAndApplyHeader(new UnsafeBuffer(payload), 0, new MessageHeaderDecoder());
      if (decoder.takerOrderId() != takerOrderId || decoder.makerOrderId() != makerOrderId) {
        continue;
      }
      if (priceTick != Long.MIN_VALUE && decoder.priceTick() != priceTick) {
        continue;
      }
      if (qty != Long.MIN_VALUE && decoder.qty() != qty) {
        continue;
      }
      return true;
    }
    return false;
  }

  private long notional(long takerOrderId) {
    long spent = 0L;
    for (byte[] payload : trades) {
      if (templateId(payload) != TradeDecoder.TEMPLATE_ID) {
        continue;
      }
      TradeDecoder decoder = new TradeDecoder();
      decoder.wrapAndApplyHeader(new UnsafeBuffer(payload), 0, new MessageHeaderDecoder());
      if (decoder.takerOrderId() == takerOrderId) {
        spent += decoder.priceTick() * decoder.qty();
      }
    }
    return spent;
  }

  private static com.kai.matchengine.orderbook.OrderBook book(EngineRegistry registry, int symbolId) {
    MatchingEngine engine = registry.get(symbolId);
    assertNotNull(engine);
    return engine.book();
  }

  private static int templateId(byte[] payload) {
    MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(new UnsafeBuffer(payload), 0);
    return header.templateId();
  }

  private static int symbolId() {
    int id = (int) (System.currentTimeMillis() % 1_000_000_000L);
    return id <= 0 ? 1 : id;
  }

  private static boolean portOpen(String host, int port) {
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress(host, port), 500);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private static HikariDataSource mysql() {
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
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-mktbuy-end-" + UUID.randomUUID());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
      TopicPartition tp = new TopicPartition(topic, partition);
      consumer.assign(List.of(tp));
      return consumer.endOffsets(List.of(tp)).get(tp);
    }
  }

  private static KafkaProducer<byte[], byte[]> ingressProducer() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "docker-mktbuy-acceptance-it");
    return new KafkaProducer<>(props);
  }

  private static KafkaConsumer<byte[], byte[]> outboundConsumer() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "docker-mktbuy-acceptance-" + UUID.randomUUID());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    return new KafkaConsumer<>(props);
  }

  private record PlaceView(
      long orderId, boolean accepted, long filledQty, long remainingQty, boolean resting, String rejectReason) {}
}
