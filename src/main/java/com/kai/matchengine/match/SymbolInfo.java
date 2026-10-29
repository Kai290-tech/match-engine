package com.kai.matchengine.match;

/** 交易对元数据（快照 / 路由用）。 */
public record SymbolInfo(int symbolId, String code, SymbolStatus status) {

  public SymbolInfo {
    if (symbolId <= 0) {
      throw new IllegalArgumentException("symbolId must be > 0");
    }
    if (code == null || code.isBlank()) {
      throw new IllegalArgumentException("code required");
    }
    if (status == null) {
      throw new IllegalArgumentException("status required");
    }
    code = code.trim();
  }

  public boolean canPlace() {
    return status == SymbolStatus.TRADING;
  }

  public boolean canCancel() {
    return status == SymbolStatus.TRADING || status == SymbolStatus.HALTED;
  }

  public SymbolInfo withStatus(SymbolStatus newStatus) {
    return new SymbolInfo(symbolId, code, newStatus);
  }
}
