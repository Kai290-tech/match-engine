package com.kai.matchengine.snapshot.delta;

import com.kai.matchengine.match.EngineRegistry;
import com.kai.matchengine.match.MatchingEngine;
import com.kai.matchengine.match.SymbolInfo;
import com.kai.matchengine.orderbook.RestingOrder;
import com.kai.matchengine.snapshot.EngineSnapshot;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** 从 live {@link EngineRegistry} 重建 materializer（GAP 恢复 / 全量 export 前）。 */
public final class MaterializerRegistrySync {

  private MaterializerRegistrySync() {}

  /** snapshot 线程 O(N)：从权威 OrderBook 灌镜像。 */
  public static void rebuildFromRegistry(EngineRegistry registry, SnapshotMaterializer materializer) {
    Objects.requireNonNull(registry, "registry");
    Objects.requireNonNull(materializer, "materializer");
    for (Integer symbolId : registry.symbolIds()) {
      SymbolInfo info = registry.symbols().get(symbolId);
      MatchingEngine engine = registry.get(symbolId);
      if (info == null || engine == null) {
        continue;
      }
      List<EngineSnapshot.RestingOrderState> orders = new ArrayList<>();
      for (RestingOrder o : engine.restingOrders()) {
        orders.add(
            EngineSnapshot.RestingOrderState.of(
                o.orderId(), o.userId(), o.side(), o.priceTick(), o.qty()));
      }
      materializer.loadSymbol(
          new EngineSnapshot.SymbolState(
              symbolId,
              info.code(),
              info.status(),
              engine.lastEventId(),
              engine.lastBookVersion(),
              orders,
              engine.recentOrderIdsOldestFirst()));
    }
  }
}
