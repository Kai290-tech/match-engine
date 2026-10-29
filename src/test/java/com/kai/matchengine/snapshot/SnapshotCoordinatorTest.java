package com.kai.matchengine.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kai.matchengine.event.inbound.OrderType;
import com.kai.matchengine.event.inbound.PlaceOrder;
import com.kai.matchengine.match.EngineRegistry;
import com.kai.matchengine.orderbook.Side;
import com.kai.matchengine.outbound.OutboundWatermark;
import com.kai.matchengine.snapshot.delta.InMemoryBookDeltaBatchStore;
import com.kai.matchengine.snapshot.delta.MatchDeltaContext;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class SnapshotCoordinatorTest {

  private SnapshotCoordinator coordinator;

  @AfterEach
  void tearDown() {
    if (coordinator != null) {
      coordinator.close();
    }
  }

  @Test
  void deltaBatch_persistedAfterTriggerAndWatermark() throws Exception {
    OutboundWatermark watermark = new OutboundWatermark();
    AtomicReference<Throwable> fatal = new AtomicReference<>();
    SnapshotTestFixtures.CoordinatorBundle bundle =
        SnapshotTestFixtures.newCoordinator(
            watermark, (m, c) -> fatal.set(c), 1, 1, 2, 5_000);
    coordinator = bundle.coordinator();
    EngineRegistry registry = bundle.registry();
    registry.register(1);

    coordinator.bootstrapFromStore();
    coordinator.start();

    MatchDeltaContext.setOffset(0);
    registry.apply(new PlaceOrder(1, 1L, 1, 1, Side.BID, OrderType.LIMIT, 100, 5));
    MatchDeltaContext.clear();
    coordinator.onMatched(0L, System.currentTimeMillis());
    coordinator.requestSnapshot();
    watermark.advanceTo(0L);

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (coordinator.baseSnapshotId() == 0L && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }

    MatchDeltaContext.setOffset(1);
    registry.apply(new PlaceOrder(1, 2L, 2, 1, Side.ASK, OrderType.LIMIT, 200, 3));
    MatchDeltaContext.clear();
    coordinator.onMatched(1L, System.currentTimeMillis());
    Thread.sleep(30);
    // watermark 未推进到 1 前不应落库成功
    assertTrue(coordinator.lastBatchToOffset() < 1L);

    watermark.advanceTo(1L);
    deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (coordinator.lastBatchToOffset() < 1L && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertEquals(1L, coordinator.lastBatchToOffset());
    assertEquals(null, fatal.get());
  }

  @Test
  void fullCompaction_restoresBookState() throws Exception {
    OutboundWatermark watermark = new OutboundWatermark();
    SnapshotTestFixtures.CoordinatorBundle bundle =
        SnapshotTestFixtures.newCoordinator(
            watermark, (m, c) -> {}, 10_000, 10_000, 2, 5_000);
    coordinator = bundle.coordinator();
    EngineRegistry registry = bundle.registry();
    registry.register(1);

    coordinator.bootstrapFromStore();
    coordinator.start();

    MatchDeltaContext.setOffset(0);
    registry.apply(new PlaceOrder(1, 1L, 1, 1, Side.BID, OrderType.LIMIT, 100, 5));
    MatchDeltaContext.clear();

    coordinator.onMatched(0L, System.currentTimeMillis());
    coordinator.requestSnapshot();
    watermark.advanceTo(0L);

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (coordinator.baseSnapshotId() == 0L && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertTrue(coordinator.baseSnapshotId() > 0L);

    EngineRegistry restored = new EngineRegistry();
    assertEquals(
        0L,
        new SnapshotRestorer(bundle.snapshotStore())
            .restoreLatest(restored, "match_event", 0)
            .orElseThrow());
    assertEquals(1, restored.get(1).book().orderCount());
    assertEquals(100, restored.get(1).book().bestBid());
  }

  @Test
  void everyNOffsets_persistsFullSnapshotWithoutExplicitRequest() throws Exception {
    OutboundWatermark watermark = new OutboundWatermark();
    AtomicReference<Throwable> fatal = new AtomicReference<>();
    SnapshotTestFixtures.CoordinatorBundle bundle =
        SnapshotTestFixtures.newCoordinator(
            watermark,
            (m, c) -> fatal.set(c),
            "match_event",
            0,
            new InMemoryBookDeltaBatchStore(),
            10_000,
            10_000,
            0.30,
            2,
            5_000,
            2L);
    coordinator = bundle.coordinator();
    bundle.registry().register(1);

    coordinator.bootstrapFromStore();
    coordinator.start();

    coordinator.onMatched(0L, 1_000L);
    Thread.sleep(80);
    assertEquals(0, bundle.snapshotStore().size());

    coordinator.onMatched(1L, 1_001L);
    watermark.advanceTo(0L);
    watermark.advanceTo(1L);
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (bundle.snapshotStore().size() < 1 && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertEquals(1, bundle.snapshotStore().size());
    assertEquals(
        1L,
        bundle.snapshotStore().loadLatest("match_event", 0).orElseThrow().lastFullyProcessedOffset());

    coordinator.onMatched(2L, 1_002L);
    Thread.sleep(80);
    assertEquals(1, bundle.snapshotStore().size());

    coordinator.onMatched(3L, 1_003L);
    watermark.advanceTo(2L);
    watermark.advanceTo(3L);
    deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (bundle.snapshotStore().loadLatest("match_event", 0).orElseThrow().lastFullyProcessedOffset() < 3L
        && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertEquals(
        3L,
        bundle.snapshotStore().loadLatest("match_event", 0).orElseThrow().lastFullyProcessedOffset());
    assertEquals(null, fatal.get());
  }

  @Test
  void singleFlight_secondFullRequestWaitsForFirst() throws Exception {
    OutboundWatermark watermark = new OutboundWatermark();
    SnapshotTestFixtures.CoordinatorBundle bundle =
        SnapshotTestFixtures.newCoordinator(
            watermark, (m, c) -> {}, 10_000, 10_000, 2, 60_000);
    coordinator = bundle.coordinator();
    bundle.registry().register(1);

    coordinator.bootstrapFromStore();
    coordinator.start();

    coordinator.onMatched(0L, 1_000L);
    coordinator.requestSnapshot();
    Thread.sleep(30);
    assertEquals(SnapshotCoordinator.State.BUSY, coordinator.state());

    coordinator.onMatched(1L, 1_001L);
    coordinator.requestSnapshot();
    Thread.sleep(30);
    assertEquals(0, bundle.snapshotStore().size());
    assertEquals(SnapshotCoordinator.State.BUSY, coordinator.state());

    watermark.advanceTo(0L);
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (bundle.snapshotStore().size() < 1 && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertEquals(1, bundle.snapshotStore().size());
    watermark.advanceTo(1L);
    deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (coordinator.state() != SnapshotCoordinator.State.IDLE && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertEquals(SnapshotCoordinator.State.IDLE, coordinator.state());
    assertTrue(bundle.snapshotStore().size() >= 1);
  }

  @Test
  void retainLatest_twoSnapshots() {
    InMemorySnapshotStore store = new InMemorySnapshotStore();
    SnapshotCodec codec = new SnapshotCodec();
    EngineRegistry registry = new EngineRegistry();
    registry.register(1);
    SnapshotPreparer preparer = new SnapshotPreparer(registry);
    for (long off = 0; off < 3; off++) {
      EngineSnapshot snap = preparer.prepare("match_event", 0, off, 1L);
      store.save(
          snap,
          java.util.List.of(
              new SnapshotStore.Section(
                  SnapshotCodec.SECTION_ENGINE_META,
                  "",
                  SnapshotCodec.ENCODING_JSON_GZIP,
                  codec.encodeMeta(snap)),
              new SnapshotStore.Section(
                  SnapshotCodec.SECTION_MATCH_SYMBOL,
                  "1",
                  SnapshotCodec.ENCODING_JSON_GZIP,
                  codec.encodeSymbol(snap.symbols().getFirst()))));
      store.retainLatest("match_event", 0, 2);
    }
    assertEquals(2, store.size());
    assertEquals(2L, store.loadLatest("match_event", 0).orElseThrow().lastFullyProcessedOffset());
  }
}
