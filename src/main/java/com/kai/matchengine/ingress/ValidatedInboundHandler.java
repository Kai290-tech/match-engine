package com.kai.matchengine.ingress;

/** 合法入站回调；非法消息不会调用。 */
@FunctionalInterface
public interface ValidatedInboundHandler {

  void onValidated(ValidatedInbound inbound);
}
