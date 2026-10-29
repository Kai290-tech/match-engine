package com.kai.matchengine.outbound;

/**
 * 异步发送一条 Kafka 记录；完成时回调（成功或失败）。
 *
 * <p>实现方可在失败时内部重试，最终仍失败则 {@code onComplete(false, error)}。
 */
@FunctionalInterface
public interface AsyncRecordSender {

  @FunctionalInterface
  interface Completion {
    void onComplete(boolean success, Throwable error);
  }

  void sendAsync(String topic, byte[] key, byte[] value, Completion completion);
}
