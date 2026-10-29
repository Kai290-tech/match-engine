package com.kai.matchengine.match;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kai.matchengine.event.inbound.OrderType;
import com.kai.matchengine.event.inbound.PlaceOrder;
import com.kai.matchengine.event.outbound.CommandResult;
import com.kai.matchengine.event.outbound.OutboundEvent;
import com.kai.matchengine.orderbook.Side;
import java.util.List;
import org.junit.jupiter.api.Test;

class EngineRegistryTest {

  @Test
  void routesBySymbolId_isolatedBooks() {
    EngineRegistry registry = new EngineRegistry(SystemTimeSource.INSTANCE);
    registry.register(1);
    registry.register(2);

    registry.apply(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 5));
    registry.apply(new PlaceOrder(2, 2, 2, 1, Side.ASK, OrderType.LIMIT, 200, 3));

    assertEquals(1, registry.get(1).book().orderCount());
    assertEquals(1, registry.get(2).book().orderCount());
    assertEquals(100, registry.get(1).book().bestBid());
    assertEquals(200, registry.get(2).book().bestAsk());

    List<OutboundEvent> out =
        registry.apply(new PlaceOrder(1, 3, 3, 2, Side.ASK, OrderType.LIMIT, 100, 5));
    assertTrue(out.getLast() instanceof CommandResult.Place place && place.accepted());
    assertEquals(0, registry.get(1).book().orderCount());
    assertEquals(1, registry.get(2).book().orderCount());
  }

  @Test
  void apply_unknownSymbol_returnsReject() {
    EngineRegistry registry = new EngineRegistry(SystemTimeSource.INSTANCE);
    registry.register(1);
    List<OutboundEvent> out =
        registry.apply(new PlaceOrder(2, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 1));
    assertTrue(out.getFirst() instanceof CommandResult.Place p && !p.accepted());
    assertEquals(
        com.kai.matchengine.event.outbound.RejectReason.UNKNOWN_SYMBOL,
        ((CommandResult.Place) out.getFirst()).rejectReason());
  }

  @Test
  void register_invalidSymbolId_throws() {
    EngineRegistry registry = new EngineRegistry(SystemTimeSource.INSTANCE);
    assertThrows(IllegalArgumentException.class, () -> registry.register(0));
  }
}
