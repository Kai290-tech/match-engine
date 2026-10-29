package com.kai.matchengine.event.outbound;

/** 拒单 / 管理命令失败原因。 */
public enum RejectReason {
  INVALID_QTY,
  INVALID_PRICE,
  DUPLICATE_ORDER_ID,
  ORDER_NOT_FOUND,
  USER_MISMATCH,
  UNKNOWN_SYMBOL,
  SYMBOL_NOT_TRADABLE,
  SYMBOL_ALREADY_EXISTS,
  INVALID_SYMBOL_CODE,
  SYMBOL_HAS_ORDERS,
  SYMBOL_CODE_IMMUTABLE,
  /** 该 symbol 订单池/簿容量已满，无法挂新单。 */
  BOOK_FULL
}
