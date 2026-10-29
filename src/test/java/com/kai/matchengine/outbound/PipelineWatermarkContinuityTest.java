package com.kai.matchengine.outbound;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kai.matchengine.event.inbound.OrderType;
import com.kai.matchengine.event.inbound.PlaceOrder;
import com.kai.matchengine.ingress.IngressValidator;
import com.kai.matchengine.ingress.MatchEventProcessor;
import com.kai.matchengine.ingress.SbeMatchEventCodec;
import com.kai.matchengine.ingress.SbeMatchEventEncoder;
import com.kai.matchengine.ingress.disruptor.IngressDisruptorPublisher;
import com.kai.matchengine.ingress.disruptor.MatchIngressDisruptor;
import com.kai.matchengine.match.EngineRegistry;
import com.kai.matchengine.match.SystemTimeSource;
import com.kai.matchengine.orderbook.Side;
import com.kai.matchengine.outbound.OutboundWatermark;
import com.kai.matchengine.snapshot.SnapshotCoordinator;
import com.kai.matchengine.snapshot.SnapshotTestFixtures;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * 链路：accept/reject → Disruptor（同序）→ outbound；watermark 不断档。
 */
class PipelineWatermarkContinuityTest {

  private MatchIngressDisruptor disruptor;
  private OutboundPublisher publisher;
  private SnapshotCoordinator snapshotCoordinator;

  @AfterEach
  void tearDown() {
    if (disruptor != null) {
      disruptor.close();
    }
    if (publisher != null) {
      publisher.close();
    }
    if (snapshotCoordinator != null) {
      snapshotCoordinator.close();
    }
  }

  @Test
  void rejectedOffset_doesNotBreakWatermarkContiguity() throws Exception {
    OutboundWatermark watermark = new OutboundWatermark();
    watermark.seedBefore(0);
    RecordingAsyncRecordSender sender = new RecordingAsyncRecordSender(Runnable::run);
    AtomicReference<Throwable> fatal = new AtomicReference<>();

    publisher =
        new OutboundPublisher(
            64,
            watermark,
            sender,
            OutboundTopics.DEFAULTS,
            new SbeOutboundEncoder(),
            (m, c) -> fatal.set(c),
            5_000);
    publisher.start();

    SnapshotTestFixtures.CoordinatorBundle bundle =
        SnapshotTestFixtures.newCoordinator(
            watermark, (m, c) -> fatal.set(c), 10_000, 10_000, 2, 5_000);
    EngineRegistry registry = bundle.registry();
    registry.register(1);
    snapshotCoordinator = bundle.coordinator();
    snapshotCoordinator.bootstrapFromStore();
    snapshotCoordinator.start();

    disruptor =
        new MatchIngressDisruptor(
            registry,
            1024,
            MatchIngressDisruptor.WaitMode.BLOCKING,
            null,
            new MatchPipelineOutboundListener(
                publisher, snapshotCoordinator, "match_event", 0, System::currentTimeMillis),
            (m, c) -> fatal.set(c));
    disruptor.start();

    MatchEventProcessor processor =
        new MatchEventProcessor(
            new SbeMatchEventCodec(),
            new IngressValidator(),
            disruptor.publisher(),
            (m, c) -> fatal.set(c),
            disruptor.publisher()::onRejected);

    SbeMatchEventEncoder encoder = new SbeMatchEventEncoder();

    // offset 0 accept
    assertTrue(
        processor.process(
            encoder.encode(new PlaceOrder(1, 1L, 1, 1, Side.BID, OrderType.LIMIT, 100, 1)),
            "match_event",
            0,
            0L));
    assertTrue(watermark.awaitAtLeast(0, 3, TimeUnit.SECONDS));

    // offset 1 reject (decode/structural) — empty outbound batch keeps watermark contiguous
    assertTrue(!processor.process(new byte[] {1, 2, 3}, "match_event", 0, 1L));
    assertTrue(watermark.awaitAtLeast(1, 3, TimeUnit.SECONDS));

    // offset 2 accept
    assertTrue(
        processor.process(
            encoder.encode(new PlaceOrder(1, 3L, 3, 1, Side.ASK, OrderType.LIMIT, 200, 1)),
            "match_event",
            0,
            2L));

    assertTrue(watermark.awaitAtLeast(2, 3, TimeUnit.SECONDS));
    assertEquals(2L, watermark.ackedOffset());
    assertEquals(null, fatal.get());
    assertEquals(2, registry.get(1).book().orderCount());
  }

  @Test
  void rejectedOffset_staysBehindSlowAcceptOnDisruptorThread() throws Exception {
    OutboundWatermark watermark = new OutboundWatermark();
    watermark.seedBefore(0);
    RecordingAsyncRecordSender sender = new RecordingAsyncRecordSender(Runnable::run);
    AtomicReference<Throwable> fatal = new AtomicReference<>();
    CountDownLatch blockOffset0 = new CountDownLatch(1);
    CountDownLatch offset0Entered = new CountDownLatch(1);

    publisher =
        new OutboundPublisher(
            64,
            watermark,
            sender,
            OutboundTopics.DEFAULTS,
            new SbeOutboundEncoder(),
            (m, c) -> fatal.set(c),
            5_000);
    publisher.start();

    SnapshotTestFixtures.CoordinatorBundle bundle =
        SnapshotTestFixtures.newCoordinator(
            watermark, (m, c) -> fatal.set(c), 10_000, 10_000, 2, 5_000);
    EngineRegistry registry = bundle.registry();
    registry.register(1);
    snapshotCoordinator = bundle.coordinator();
    snapshotCoordinator.bootstrapFromStore();
    snapshotCoordinator.start();

    disruptor =
        new MatchIngressDisruptor(
            registry,
            1024,
            MatchIngressDisruptor.WaitMode.BLOCKING,
            null,
            (symbolId, offset, events) -> {
              if (offset == 0L) {
                offset0Entered.countDown();
                try {
                  assertTrue(blockOffset0.await(3, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new RuntimeException(e);
                }
              }
              publisher.enqueueUninterruptibly(
                  new OutboundBatch("match_event", 0, offset, symbolId, events));
            },
            (m, c) -> fatal.set(c));
    disruptor.start();

    IngressDisruptorPublisher ingressPublisher = disruptor.publisher();
    MatchEventProcessor processor =
        new MatchEventProcessor(
            new SbeMatchEventCodec(),
            new IngressValidator(),
            ingressPublisher,
            (m, c) -> fatal.set(c),
            ingressPublisher::onRejected);

    SbeMatchEventEncoder encoder = new SbeMatchEventEncoder();
    assertTrue(
        processor.process(
            encoder.encode(new PlaceOrder(1, 1L, 1, 1, Side.BID, OrderType.LIMIT, 100, 1)),
            "match_event",
            0,
            0L));
    assertTrue(offset0Entered.await(3, TimeUnit.SECONDS));

    assertFalse(processor.process(new byte[] {1, 2, 3}, "match_event", 0, 1L));
    Thread.sleep(100);
    assertEquals(-1L, watermark.ackedOffset(), "reject must not skip ahead of slow offset 0");

    blockOffset0.countDown();
    assertTrue(watermark.awaitAtLeast(1, 3, TimeUnit.SECONDS));

    assertTrue(
        processor.process(
            encoder.encode(new PlaceOrder(1, 3L, 3, 1, Side.ASK, OrderType.LIMIT, 200, 1)),
            "match_event",
            0,
            2L));
    assertTrue(watermark.awaitAtLeast(2, 3, TimeUnit.SECONDS));
    assertEquals(null, fatal.get());
  }
}
