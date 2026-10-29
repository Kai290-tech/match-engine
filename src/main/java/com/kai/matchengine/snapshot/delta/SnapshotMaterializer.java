package com.kai.matchengine.snapshot.delta;

import com.kai.matchengine.match.SymbolInfo;
import com.kai.matchengine.match.SymbolStatus;
import com.kai.matchengine.snapshot.EngineSnapshot;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 多 symbol 订单簿镜像：仅 snapshot 线程访问，由 {@link BookDelta} 驱动。
 *
 * <p>用于增量 batch 与全量 export，不在撮合线程扫 {@link com.kai.matchengine.orderbook.OrderBook}。
 */
public final class SnapshotMaterializer {

  private final Map<Integer, MaterializedSymbolState> symbols = new HashMap<>();
  private final Map<Integer, SymbolInfo> symbolMeta = new HashMap<>();

  /** 从全量快照恢复后灌入基底（恢复路径 / compaction 后）。 */
  public void loadSymbol(EngineSnapshot.SymbolState state) {
    Objects.requireNonNull(state, "state");
    MaterializedSymbolState mat = symbols.computeIfAbsent(state.symbolId(), id -> new MaterializedSymbolState());
    mat.loadFrom(state);
    symbolMeta.put(
        state.symbolId(),
        new SymbolInfo(state.symbolId(), state.code(), state.status()));
  }

  public void registerSymbolMeta(SymbolInfo info) {
    Objects.requireNonNull(info, "info");
    symbolMeta.put(info.symbolId(), info);
    symbols.computeIfAbsent(info.symbolId(), id -> new MaterializedSymbolState());
  }

  public void apply(BookDelta delta) {
    Objects.requireNonNull(delta, "delta");
    if (delta.type() == BookDeltaType.META_SYMBOL_STATUS) {
      setSymbolStatus(delta.symbolId(), delta.toSymbolStatus());
      return;
    }
    MaterializedSymbolState mat =
        symbols.computeIfAbsent(delta.symbolId(), id -> new MaterializedSymbolState());
    mat.apply(delta);
  }

  /** 消费 {@link BookDeltaLog} 中全部待处理 delta。 */
  public int drain(BookDeltaLog log) {
    Objects.requireNonNull(log, "log");
    int n = 0;
    BookDelta delta;
    while ((delta = log.poll()) != null) {
      apply(delta);
      n++;
    }
    return n;
  }

  public int orderCount(int symbolId) {
    MaterializedSymbolState mat = symbols.get(symbolId);
    return mat == null ? 0 : mat.orderCount();
  }

  public EngineSnapshot.SymbolState exportSymbolState(int symbolId) {
    MaterializedSymbolState mat = symbols.get(symbolId);
    SymbolInfo meta = symbolMeta.get(symbolId);
    if (mat == null || meta == null) {
      throw new IllegalArgumentException("unknown symbolId: " + symbolId);
    }
    return new EngineSnapshot.SymbolState(
        symbolId,
        meta.code(),
        meta.status(),
        mat.eventId(),
        mat.bookVersion(),
        mat.restingOrders(),
        mat.recentOrderIdsOldestFirst());
  }

  public List<EngineSnapshot.SymbolState> exportAllSymbolStates() {
    return symbolMeta.keySet().stream().sorted().map(this::exportSymbolState).toList();
  }

  public void setSymbolStatus(int symbolId, SymbolStatus status) {
    SymbolInfo prev = symbolMeta.get(symbolId);
    if (prev == null) {
      throw new IllegalArgumentException("unknown symbolId: " + symbolId);
    }
    symbolMeta.put(symbolId, new SymbolInfo(prev.symbolId(), prev.code(), status));
  }
}
