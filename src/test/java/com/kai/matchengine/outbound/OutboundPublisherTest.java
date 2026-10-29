package com.kai.matchengine.outbound;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kai.matchengine.event.inbound.OrderType;
import com.kai.matchengine.event.inbound.PlaceOrder;
import com.kai.matchengine.event.outbound.CommandResult;
import com.kai.matchengine.match.EngineRegistry;
import com.kai.matchengine.match.SystemTimeSource;
import com.kai.matchengine.orderbook.Side;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class OutboundPublisherTest {

  private OutboundPublisher publisher;

  @AfterEach
  void tearDown() {
    if (publisher != null) {
      publisher.close();
    }
  }

  @Test
  void watermark_advancesContiguously() {
    OutboundWatermark w = new OutboundWatermark();
    assertEquals(-1L, w.ackedOffset());
    w.advanceTo(0);
    w.advanceTo(1);
    assertEquals(1L, w.ackedOffset());
    assertThrows(IllegalStateException.class, () -> w.advanceTo(3));
  }

  @Test
  void batch_allSent_thenWatermarkAdvances() throws Exception {
    OutboundWatermark watermark = new OutboundWatermark();
    RecordingAsyncRecordSender sender =
        new RecordingAsyncRecordSender(Runnable::run);
    AtomicReference<Throwable> fatal = new AtomicReference<>();
    publisher =
        new OutboundPublisher(
            16,
            watermark,
            sender,
            OutboundTopics.DEFAULTS,
            new SbeOutboundEncoder(),
            (m, c) -> fatal.set(c),
            5_000);
    publisher.start();

    EngineRegistry registry = new EngineRegistry(SystemTimeSource.INSTANCE);
    registry.register(1);
    var events =
        registry.apply(new PlaceOrder(1, 42L, 1, 1, Side.BID, OrderType.LIMIT, 100, 1));
    publisher.enqueue(new OutboundBatch("match_event", 0, 0L, 1, events));

    assertTrue(watermark.awaitAtLeast(0, 2, TimeUnit.SECONDS));
    assertTrue(sender.sent().size() >= 1);
    assertTrue(
        sender.sent().stream().anyMatch(s -> s.topic().equals("event_result")));
    assertEquals(null, fatal.get());
  }

  @Test
  void batch_sendFailure_isFatal_andDoesNotAdvance() throws Exception {
    OutboundWatermark watermark = new OutboundWatermark();
    RecordingAsyncRecordSender sender =
        new RecordingAsyncRecordSender(Runnable::run);
    sender.setSuccessWhen(s -> false);
    AtomicReference<String> fatalMsg = new AtomicReference<>();
    publisher =
        new OutboundPublisher(
            16,
            watermark,
            sender,
            OutboundTopics.DEFAULTS,
            new SbeOutboundEncoder(),
            (m, c) -> fatalMsg.set(m),
            5_000);
    publisher.start();

    publisher.enqueue(
        new OutboundBatch(
            "match_event",
            0,
            0L,
            1,
            List.of(
                CommandResult.Place.ok(1L, 1L, "S-1", 1L, 0L, 1L, true))));

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (fatalMsg.get() == null && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertTrue(fatalMsg.get() != null && fatalMsg.get().contains("failed"));
    assertEquals(-1L, watermark.ackedOffset());
  }

  @Test
  void emptyBatch_stillAdvancesWatermark() throws Exception {
    OutboundWatermark watermark = new OutboundWatermark();
    AtomicReference<Throwable> fatal = new AtomicReference<>();
    publisher =
        new OutboundPublisher(
            8,
            watermark,
            new RecordingAsyncRecordSender(Runnable::run),
            OutboundTopics.DEFAULTS,
            new SbeOutboundEncoder(),
            (m, c) -> fatal.set(c),
            1_000);
    publisher.start();
    publisher.enqueue(new OutboundBatch("match_event", 0, 0L, 1, List.of()));
    assertTrue(watermark.awaitAtLeast(0, 2, TimeUnit.SECONDS));
    assertEquals(null, fatal.get());
  }
  @Test
  void seedBefore_allowsNonZeroStart() {
    OutboundWatermark w = new OutboundWatermark();
    w.seedBefore(1000);
    assertEquals(999L, w.ackedOffset());
    w.advanceTo(1000);
    assertEquals(1000L, w.ackedOffset());
    assertThrows(IllegalStateException.class, () -> w.seedBefore(2000));
  }

}
