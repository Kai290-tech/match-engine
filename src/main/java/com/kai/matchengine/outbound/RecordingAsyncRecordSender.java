package com.kai.matchengine.outbound;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.function.Predicate;

/** 单测用：可配置成功/失败的异步 sender。 */
public final class RecordingAsyncRecordSender implements AsyncRecordSender {

  public record Sent(String topic, byte[] key, byte[] value) {}

  private final List<Sent> sent = new CopyOnWriteArrayList<>();
  private final Executor callbackExecutor;
  private volatile Predicate<Sent> successWhen = s -> true;

  public RecordingAsyncRecordSender(Executor callbackExecutor) {
    this.callbackExecutor = callbackExecutor;
  }

  public void setSuccessWhen(Predicate<Sent> successWhen) {
    this.successWhen = successWhen;
  }

  public List<Sent> sent() {
    return new ArrayList<>(sent);
  }

  @Override
  public void sendAsync(String topic, byte[] key, byte[] value, Completion completion) {
    Sent record = new Sent(topic, key, value);
    sent.add(record);
    boolean ok = successWhen.test(record);
    callbackExecutor.execute(
        () -> completion.onComplete(ok, ok ? null : new IllegalStateException("forced fail")));
  }
}
