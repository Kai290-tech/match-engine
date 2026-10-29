package com.kai.matchengine.snapshot.delta;

/** 订单簿镜像增量事件类型。 */
public enum BookDeltaType {
  REST_ADD,
  REST_REMOVE,
  REST_QTY,
  META_RECENT_ORDER,
  META_SEQUENCES,
  /** symbol 管理：side 字段存 {@link com.kai.matchengine.match.SymbolStatus#ordinal()}。 */
  META_SYMBOL_STATUS
}
