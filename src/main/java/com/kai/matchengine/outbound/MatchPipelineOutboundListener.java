package com.kai.matchengine.outbound;

import com.kai.matchengine.event.outbound.OutboundEvent;
import com.kai.matchengine.ingress.disruptor.MatchingApplyHandler.OutboundListener;
import com.kai.matchengine.snapshot.SnapshotCoordinator;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 撮合完成后：出站入队 + 通知快照协调器（触发条件在 coordinator 内判断）。
 */
public final class MatchPipelineOutboundListener implements OutboundListener {

  private static final Logger log = LoggerFactory.getLogger(MatchPipelineOutboundListener.class);

  private final OutboundPublisher publisher;
  private final SnapshotCoordinator snapshotCoordinator;
  private final String matchTopic;
  private final int matchPartition;
  private final java.util.function.LongSupplier wallClockMillis;

  public MatchPipelineOutboundListener(
      OutboundPublisher publisher,
      SnapshotCoordinator snapshotCoordinator,
      String matchTopic,
      int matchPartition,
      java.util.function.LongSupplier wallClockMillis) {
    this.publisher = Objects.requireNonNull(publisher, "publisher");
    this.snapshotCoordinator = Objects.requireNonNull(snapshotCoordinator, "snapshotCoordinator");
    this.matchTopic = Objects.requireNonNull(matchTopic, "matchTopic");
    this.matchPartition = matchPartition;
    this.wallClockMillis = Objects.requireNonNull(wallClockMillis, "wallClockMillis");
  }

  @Override
  public void onOutbound(int symbolId, long offset, List<OutboundEvent> events) {
    log.debug(
        "enqueue outbound offset={} symbolId={} events={}", offset, symbolId, events.size());
    publisher.enqueueUninterruptibly(
        new OutboundBatch(matchTopic, matchPartition, offset, symbolId, events));
    snapshotCoordinator.onMatched(offset, wallClockMillis.getAsLong());
  }
}
