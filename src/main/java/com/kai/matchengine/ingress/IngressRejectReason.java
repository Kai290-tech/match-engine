package com.kai.matchengine.ingress;

/** 入站非法原因（不进入撮合）。 */
public enum IngressRejectReason {
  EMPTY_PAYLOAD,
  DECODE_FAILED,
  UNKNOWN_TEMPLATE,
  INVALID_SYMBOL,
  INVALID_QTY,
  INVALID_PRICE,
  INVALID_ENUM,
  INVALID_SYMBOL_CODE,
  UNKNOWN_SYMBOL
}
