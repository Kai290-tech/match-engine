package com.kai.matchengine.snapshot;

import com.kai.matchengine.match.EngineRegistry;
import com.kai.matchengine.match.MatchingEngine;
import com.kai.matchengine.match.SymbolInfo;
import com.kai.matchengine.orderbook.RestingOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** 在撮合主线程导出不可变 {@link EngineSnapshot}。 */
public final class SnapshotPreparer {

  private final EngineRegistry registry;

  public SnapshotPreparer(EngineRegistry registry) {
    this.registry = Objects.requireNonNull(registry, "registry");
  }

  public EngineSnapshot prepare(
      String matchTopic, int matchPartition, long lastFullyProcessedOffset, long preparedAtMillis) {
    List<EngineSnapshot.SymbolState> symbols = new ArrayList<>();
    for (Integer symbolId : registry.symbolIds()) {
      MatchingEngine engine = registry.get(symbolId);
      SymbolInfo info = registry.symbols().get(symbolId);
      if (engine == null || info == null) {
        continue;
      }
      List<EngineSnapshot.RestingOrderState> orders = new ArrayList<>();
      for (RestingOrder o : engine.restingOrders()) {
        orders.add(
            EngineSnapshot.RestingOrderState.of(
                o.orderId(), o.userId(), o.side(), o.priceTick(), o.qty()));
      }
      symbols.add(
          new EngineSnapshot.SymbolState(
              symbolId,
              info.code(),
              info.status(),
              engine.lastEventId(),
              engine.lastBookVersion(),
              orders,
              engine.recentOrderIdsOldestFirst()));
    }
    return new EngineSnapshot(
        EngineSnapshot.SCHEMA_VERSION,
        matchTopic,
        matchPartition,
        lastFullyProcessedOffset,
        preparedAtMillis,
        symbols);
  }
}
