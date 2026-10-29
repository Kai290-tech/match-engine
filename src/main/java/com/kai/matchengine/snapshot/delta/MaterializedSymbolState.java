package com.kai.matchengine.snapshot.delta;

import com.kai.matchengine.match.RecentOrderIdWindow;
import com.kai.matchengine.snapshot.EngineSnapshot;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** persist 线程上单 symbol 的镜像状态。 */
final class MaterializedSymbolState {

  private final LinkedHashMap<Long, EngineSnapshot.RestingOrderState> orders = new LinkedHashMap<>();
  private final RecentOrderIdWindow recentOrderIds = new RecentOrderIdWindow();
  private long eventId;
  private long bookVersion;

  void loadFrom(EngineSnapshot.SymbolState state) {
    orders.clear();
    for (EngineSnapshot.RestingOrderState o : state.orders()) {
      orders.put(o.orderId(), o);
    }
    recentOrderIds.restoreOldestFirst(state.recentOrderIds());
    eventId = state.eventId();
    bookVersion = state.bookVersion();
  }

  void apply(BookDelta delta) {
    switch (delta.type()) {
      case REST_ADD -> {
        EngineSnapshot.RestingOrderState o =
            EngineSnapshot.RestingOrderState.of(
                delta.orderId(),
                delta.userId(),
                delta.toSide(),
                delta.priceTick(),
                delta.qty());
        orders.put(delta.orderId(), o);
        if (delta.eventId() > 0) {
          eventId = delta.eventId();
        }
        if (delta.bookVersion() > 0) {
          bookVersion = delta.bookVersion();
        }
      }
      case REST_REMOVE -> {
        orders.remove(delta.orderId());
        if (delta.eventId() > 0) {
          eventId = delta.eventId();
        }
        if (delta.bookVersion() > 0) {
          bookVersion = delta.bookVersion();
        }
      }
      case REST_QTY -> {
        EngineSnapshot.RestingOrderState prev = orders.get(delta.orderId());
        if (prev == null) {
          throw new IllegalStateException(
              "REST_QTY missing order orderId=" + delta.orderId() + " symbolId=" + delta.symbolId());
        }
        orders.put(
            delta.orderId(),
            new EngineSnapshot.RestingOrderState(
                prev.orderId(),
                prev.userId(),
                prev.side(),
                prev.priceTick(),
                delta.qty()));
        if (delta.eventId() > 0) {
          eventId = delta.eventId();
        }
        if (delta.bookVersion() > 0) {
          bookVersion = delta.bookVersion();
        }
      }
      case META_RECENT_ORDER -> recentOrderIds.remember(delta.orderId());
      case META_SEQUENCES -> {
        eventId = delta.eventId();
        bookVersion = delta.bookVersion();
      }
      default -> throw new IllegalArgumentException("unknown delta type: " + delta.type());
    }
  }

  List<EngineSnapshot.RestingOrderState> restingOrders() {
    return List.copyOf(orders.values());
  }

  List<Long> recentOrderIdsOldestFirst() {
    return recentOrderIds.snapshotOldestFirst();
  }

  long eventId() {
    return eventId;
  }

  long bookVersion() {
    return bookVersion;
  }

  int orderCount() {
    return orders.size();
  }
}
