package com.kai.matchengine.snapshot.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kai.matchengine.event.inbound.OrderType;
import com.kai.matchengine.event.inbound.PlaceOrder;
import com.kai.matchengine.match.EngineRegistry;
import com.kai.matchengine.match.MatchingEngine;
import com.kai.matchengine.match.SymbolStatus;
import com.kai.matchengine.match.SystemTimeSource;
import com.kai.matchengine.orderbook.Side;
import com.kai.matchengine.snapshot.EngineSnapshot;
import java.util.List;
import org.junit.jupiter.api.Test;

class SnapshotMaterializerTest {

  @Test
  void apply_deltas_matchesEngineRestingOrders() {
    EngineRegistry registry = new EngineRegistry(SystemTimeSource.INSTANCE);
    registry.register(1, "BTC", SymbolStatus.TRADING);
    MatchingEngine engine = registry.get(1);

    BookDeltaLog log = new BookDeltaLog(64, 64);
    SnapshotMaterializer materializer = new SnapshotMaterializer();
    materializer.registerSymbolMeta(registry.symbols().get(1));

    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 5));
    log.tryAppend(
        BookDelta.restAdd(
            0, 1, 1, 1, Side.BID, 100, 5, engine.lastEventId(), engine.lastBookVersion()));
    log.tryAppend(BookDelta.metaRecentOrder(0, 1, 1));

    engine.apply(new PlaceOrder(1, 2, 2, 1, Side.ASK, OrderType.LIMIT, 200, 3));
    log.tryAppend(
        BookDelta.restAdd(
            1, 1, 2, 1, Side.ASK, 200, 3, engine.lastEventId(), engine.lastBookVersion()));
    log.tryAppend(BookDelta.metaRecentOrder(1, 1, 2));

    assertEquals(4, materializer.drain(log));

    EngineSnapshot.SymbolState exported = materializer.exportSymbolState(1);
    assertEquals(2, exported.orders().size());
    assertEquals(engine.lastEventId(), exported.eventId());
    assertEquals(engine.lastBookVersion(), exported.bookVersion());
    assertEquals(engine.recentOrderIdsOldestFirst(), exported.recentOrderIds());

    assertTrue(
        exported.orders().stream().anyMatch(o -> o.orderId() == 1 && o.qty() == 5),
        "bid order");
    assertTrue(
        exported.orders().stream().anyMatch(o -> o.orderId() == 2 && o.qty() == 3),
        "ask order");
  }

  @Test
  void loadFrom_full_thenApplyRemove() {
    SnapshotMaterializer materializer = new SnapshotMaterializer();
    EngineSnapshot.SymbolState base =
        new EngineSnapshot.SymbolState(
            1,
            "X",
            SymbolStatus.TRADING,
            10,
            20,
            List.of(EngineSnapshot.RestingOrderState.of(1, 1, Side.BID, 100, 5)),
            List.of(1L));
    materializer.loadSymbol(base);

    materializer.apply(BookDelta.restRemove(5, 1, 1, 11, 21));
    EngineSnapshot.SymbolState exported = materializer.exportSymbolState(1);
    assertEquals(0, exported.orders().size());
    assertEquals(11, exported.eventId());
    assertEquals(21, exported.bookVersion());
  }
}
