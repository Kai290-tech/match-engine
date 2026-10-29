package com.kai.matchengine.ingress;

/** 反序列化或基础校验失败。 */
public final class IngressException extends Exception {

  private final IngressRejectReason reason;

  public IngressException(IngressRejectReason reason, String message) {
    super(message);
    this.reason = reason;
  }

  public IngressException(IngressRejectReason reason, String message, Throwable cause) {
    super(message, cause);
    this.reason = reason;
  }

  public IngressRejectReason reason() {
    return reason;
  }
}
