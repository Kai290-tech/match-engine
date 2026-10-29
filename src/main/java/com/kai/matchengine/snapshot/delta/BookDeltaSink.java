package com.kai.matchengine.snapshot.delta;

/** 撮合线程写入 {@link BookDeltaLog} 的桥接。 */
public interface BookDeltaSink {

  void append(BookDelta delta);
}
