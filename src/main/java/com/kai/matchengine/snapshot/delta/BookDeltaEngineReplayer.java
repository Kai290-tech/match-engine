package com.kai.matchengine.snapshot.delta;

import com.kai.matchengine.match.EngineRegistry;
import com.kai.matchengine.match.MatchingEngine;
import com.kai.matchengine.match.SymbolStatus;
import com.kai.matchengine.orderbook.OrderNode;
import java.util.Objects;

/** 将 {@link BookDelta} 回放到 live {@link MatchingEngine}（恢复路径）。 */
public final class BookDeltaEngineReplayer {

  private BookDeltaEngineReplayer() {}

  public static void apply(EngineRegistry registry, BookDelta delta) {
    Objects.requireNonNull(registry, "registry");
    Objects.requireNonNull(delta, "delta");
    MatchingEngine engine = registry.get(delta.symbolId());
    if (engine == null) {
      throw new IllegalStateException("unknown symbolId=" + delta.symbolId());
    }
    switch (delta.type()) {
      case REST_ADD ->
          engine.restoreRestingOrder(
              delta.orderId(),
              delta.userId(),
              delta.toSide(),
              delta.priceTick(),
              delta.qty());
      case REST_REMOVE -> engine.book().cancel(delta.orderId());
      case REST_QTY -> applyRestQty(engine, delta);
      case META_RECENT_ORDER -> engine.replayRecentOrder(delta.orderId());
      case META_SYMBOL_STATUS ->
          registry.symbols().setStatus(delta.symbolId(), delta.toSymbolStatus());
      case META_SEQUENCES ->
          engine.restoreSequences(delta.eventId(), delta.bookVersion());
      default -> throw new IllegalArgumentException("unknown delta type: " + delta.type());
    }
    applySequencesIfPresent(engine, delta);
  }

  private static void applyRestQty(MatchingEngine engine, BookDelta delta) {
    OrderNode node = engine.book().get(delta.orderId());
    if (node == null) {
      throw new IllegalStateException(
          "REST_QTY missing order orderId=" + delta.orderId() + " symbolId=" + delta.symbolId());
    }
    long newQty = delta.qty();
    long oldQty = node.qty();
    if (newQty <= 0) {
      engine.book().cancel(delta.orderId());
    } else if (newQty < oldQty) {
      engine.book().reduce(node, oldQty - newQty);
    } else if (newQty != oldQty) {
      throw new IllegalStateException(
          "REST_QTY increase not supported orderId=" + delta.orderId() + " old=" + oldQty + " new=" + newQty);
    }
  }

  private static void applySequencesIfPresent(MatchingEngine engine, BookDelta delta) {
    if (delta.type() == BookDeltaType.META_SEQUENCES) {
      return;
    }
    long eventId = delta.eventId() > 0 ? delta.eventId() : engine.lastEventId();
    long bookVersion = delta.bookVersion() > 0 ? delta.bookVersion() : engine.lastBookVersion();
    if (delta.eventId() > 0 || delta.bookVersion() > 0) {
      engine.restoreSequences(eventId, bookVersion);
    }
  }
}
