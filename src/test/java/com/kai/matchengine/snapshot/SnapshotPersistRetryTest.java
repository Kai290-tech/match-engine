package com.kai.matchengine.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.kai.matchengine.event.inbound.OrderType;
import com.kai.matchengine.event.inbound.PlaceOrder;
import com.kai.matchengine.match.EngineRegistry;
import com.kai.matchengine.orderbook.Side;
import com.kai.matchengine.outbound.OutboundWatermark;
import com.kai.matchengine.snapshot.delta.BookDeltaBatchStore;
import com.kai.matchengine.snapshot.delta.InMemoryBookDeltaBatchStore;
import com.kai.matchengine.snapshot.delta.MatchDeltaContext;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class SnapshotPersistRetryTest {

  private SnapshotCoordinator coordinator;

  @AfterEach
  void tearDown() {
    if (coordinator != null) {
      coordinator.close();
    }
  }

  @Test
  void deltaBatchPersistFailure_keepsPending_retriesNextCycle() throws Exception {
    OutboundWatermark watermark = new OutboundWatermark();
    AtomicReference<Throwable> fatal = new AtomicReference<>();
    AtomicInteger saveCalls = new AtomicInteger();

    BookDeltaBatchStore failingThenOk =
        new BookDeltaBatchStore() {
          private final InMemoryBookDeltaBatchStore delegate = new InMemoryBookDeltaBatchStore();

          @Override
          public long save(
              long baseSnapshotId,
              String matchTopic,
              int matchPartition,
              long fromOffset,
              long toOffset,
              int deltaCount,
              List<Section> sections) {
            if (saveCalls.getAndIncrement() == 0) {
              throw new IllegalStateException("simulated jdbc failure");
            }
            return delegate.save(
                baseSnapshotId,
                matchTopic,
                matchPartition,
                fromOffset,
                toOffset,
                deltaCount,
                sections);
          }

          @Override
          public List<StoredBatch> loadChain(
              long baseSnapshotId, String matchTopic, int matchPartition) {
            return delegate.loadChain(baseSnapshotId, matchTopic, matchPartition);
          }

          @Override
          public void pruneThrough(
              long baseSnapshotId, String matchTopic, int matchPartition, long throughOffset) {
            delegate.pruneThrough(baseSnapshotId, matchTopic, matchPartition, throughOffset);
          }
        };

    SnapshotTestFixtures.CoordinatorBundle bundle =
        SnapshotTestFixtures.newCoordinator(
            watermark, (m, c) -> fatal.set(c), failingThenOk, 1, 1, 2, 5_000);
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
    watermark.advanceTo(1L);

    deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (coordinator.lastBatchToOffset() < 1L && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }

    assertEquals(1L, coordinator.lastBatchToOffset());
    assertNull(fatal.get(), "snapshot jdbc failure must not fatal matching");
    assertEquals(true, saveCalls.get() >= 2);
  }
}
