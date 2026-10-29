package com.kai.matchengine.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.kai.matchengine.event.inbound.OrderType;
import com.kai.matchengine.event.inbound.PlaceOrder;
import com.kai.matchengine.match.EngineRegistry;
import com.kai.matchengine.orderbook.Side;
import com.kai.matchengine.outbound.OutboundWatermark;
import com.kai.matchengine.snapshot.delta.MatchDeltaContext;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class SnapshotRestorerDeltaBatchTest {

  private SnapshotCoordinator coordinator;

  @AfterEach
  void tearDown() {
    if (coordinator != null) {
      coordinator.close();
    }
  }

  @Test
  void restoreLatest_replaysDeltaBatchChainAfterFullSnapshot() throws Exception {
    OutboundWatermark watermark = new OutboundWatermark();
    SnapshotTestFixtures.CoordinatorBundle bundle =
        SnapshotTestFixtures.newCoordinator(
            watermark, (m, c) -> {}, 1, 1, 2, 5_000);
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

    deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (coordinator.lastBatchToOffset() < 1L && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertEquals(1L, coordinator.lastBatchToOffset());

    EngineRegistry restored = new EngineRegistry();
    assertEquals(
        1L,
        new SnapshotRestorer(bundle.snapshotStore(), bundle.batchStore())
            .restoreLatest(restored, "match_event", 0)
            .orElseThrow());
    assertEquals(2, restored.get(1).book().orderCount());
    assertEquals(100, restored.get(1).book().bestBid());
    assertEquals(200, restored.get(1).book().bestAsk());
  }
}
