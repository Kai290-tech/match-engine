package com.kai.matchengine.match;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 交易对元数据表：{@code symbolId → SymbolInfo}。
 *
 * <p>由撮合 apply 线程写入；ingress 校验只做结构检查（存在性在 apply 侧拒绝）。
 */
public final class SymbolRegistry {

  private final Map<Integer, SymbolInfo> symbols = new ConcurrentHashMap<>();

  public SymbolInfo get(int symbolId) {
    return symbols.get(symbolId);
  }

  public boolean contains(int symbolId) {
    return symbols.containsKey(symbolId);
  }

  public Set<Integer> symbolIds() {
    return Set.copyOf(symbols.keySet());
  }

  public Collection<SymbolInfo> all() {
    return Set.copyOf(symbols.values());
  }

  /** 冷启动 / 快照恢复：直接写入。 */
  public void put(SymbolInfo info) {
    Objects.requireNonNull(info, "info");
    symbols.put(info.symbolId(), info);
  }

  public SymbolInfo putIfAbsent(SymbolInfo info) {
    Objects.requireNonNull(info, "info");
    return symbols.putIfAbsent(info.symbolId(), info);
  }

  public void setStatus(int symbolId, SymbolStatus status) {
    SymbolInfo cur = symbols.get(symbolId);
    if (cur == null) {
      throw new IllegalArgumentException("unknown symbolId=" + symbolId);
    }
    symbols.put(symbolId, cur.withStatus(status));
  }
}
