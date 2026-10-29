package com.kai.matchengine.snapshot.delta;

import java.util.Objects;

/** 将 {@link BookDelta} 非阻塞写入共享 {@link BookDeltaLog}。 */
public final class BookDeltaRecorder implements BookDeltaSink {

  private final BookDeltaLog log;

  public BookDeltaRecorder(BookDeltaLog log) {
    this.log = Objects.requireNonNull(log, "log");
  }

  @Override
  public void append(BookDelta delta) {
    Objects.requireNonNull(delta, "delta");
    log.tryAppend(delta);
  }

  public BookDeltaLog log() {
    return log;
  }
}
