package com.kai.matchengine.match;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kai.matchengine.event.inbound.CancelOrder;
import com.kai.matchengine.event.inbound.CreateSymbol;
import com.kai.matchengine.event.inbound.HaltSymbol;
import com.kai.matchengine.event.inbound.OrderType;
import com.kai.matchengine.event.inbound.PlaceOrder;
import com.kai.matchengine.event.inbound.ResumeSymbol;
import com.kai.matchengine.event.inbound.UpdateSymbol;
import com.kai.matchengine.event.outbound.CommandResult;
import com.kai.matchengine.event.outbound.OutboundEvent;
import com.kai.matchengine.event.outbound.RejectReason;
import com.kai.matchengine.orderbook.Side;
import com.kai.matchengine.outbound.OutboundWatermark;
import com.kai.matchengine.snapshot.SnapshotCoordinator;
import com.kai.matchengine.snapshot.SnapshotRestorer;
import com.kai.matchengine.snapshot.SnapshotTestFixtures;
import com.kai.matchengine.snapshot.delta.MatchDeltaContext;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class SymbolLifecycleTest {

  @Test
  void create_place_halt_blocksPlace_allowsCancel_resume() {
    EngineRegistry registry = new EngineRegistry(SystemTimeSource.INSTANCE);

    List<OutboundEvent> created =
        registry.apply(new CreateSymbol(1, 1L, "BTCUSDT"));
    CommandResult.Symbol cr = assertInstanceOf(CommandResult.Symbol.class, created.get(0));
    assertTrue(cr.accepted());
    assertEquals("BTCUSDT", cr.symbol());
    assertEquals(SymbolStatus.TRADING, cr.status());

    List<OutboundEvent> placed =
        registry.apply(new PlaceOrder(1, 2L, 10, 1, Side.BID, OrderType.LIMIT, 100, 5));
    assertTrue(((CommandResult) placed.get(placed.size() - 1)).accepted());
    assertEquals("BTCUSDT", registry.get(1).symbol());

    registry.apply(new HaltSymbol(1, 3L));
    assertEquals(SymbolStatus.HALTED, registry.symbols().get(1).status());

    List<OutboundEvent> blocked =
        registry.apply(new PlaceOrder(1, 4L, 11, 1, Side.BID, OrderType.LIMIT, 100, 1));
    CommandResult.Place pr = assertInstanceOf(CommandResult.Place.class, blocked.get(0));
    assertFalse(pr.accepted());
    assertEquals(RejectReason.SYMBOL_NOT_TRADABLE, pr.rejectReason());

    List<OutboundEvent> cancelled = registry.apply(new CancelOrder(1, 5L, 10, 1));
    assertTrue(((CommandResult.Cancel) cancelled.get(cancelled.size() - 1)).accepted());

    registry.apply(new ResumeSymbol(1, 6L));
    List<OutboundEvent> again =
        registry.apply(new PlaceOrder(1, 7L, 12, 1, Side.ASK, OrderType.LIMIT, 101, 1));
    assertTrue(((CommandResult) again.get(again.size() - 1)).accepted());
  }

  @Test
  void updateSymbol_changesCode_whenBookEmpty() {
    EngineRegistry registry = new EngineRegistry(SystemTimeSource.INSTANCE);
    registry.apply(new CreateSymbol(1, 1L, "BTCUSDT"));
    CommandResult.Symbol same =
        assertInstanceOf(
            CommandResult.Symbol.class,
            registry.apply(new UpdateSymbol(1, 2L, "BTCUSDT")).get(0));
    assertTrue(same.accepted());
    assertEquals("BTCUSDT", registry.symbols().get(1).code());

    CommandResult.Symbol blocked =
        assertInstanceOf(
            CommandResult.Symbol.class,
            registry.apply(new UpdateSymbol(1, 3L, "ETHUSDT")).get(0));
    assertFalse(blocked.accepted());
    assertEquals(RejectReason.SYMBOL_CODE_IMMUTABLE, blocked.rejectReason());
  }

  @Test
  void createDuplicate_rejected() {
    EngineRegistry registry = new EngineRegistry(SystemTimeSource.INSTANCE);
    registry.apply(new CreateSymbol(1, 1L, "AAA"));
    CommandResult.Symbol dup =
        assertInstanceOf(
            CommandResult.Symbol.class, registry.apply(new CreateSymbol(1, 2L, "BBB")).get(0));
    assertFalse(dup.accepted());
    assertEquals(RejectReason.SYMBOL_ALREADY_EXISTS, dup.rejectReason());
  }

  @Test
  void placeUnknown_rejectedAtApply() {
    EngineRegistry registry = new EngineRegistry(SystemTimeSource.INSTANCE);
    CommandResult.Place pr =
        assertInstanceOf(
            CommandResult.Place.class,
            registry
                .apply(new PlaceOrder(9, 1L, 1, 1, Side.BID, OrderType.LIMIT, 100, 1))
                .get(0));
    assertEquals(RejectReason.UNKNOWN_SYMBOL, pr.rejectReason());
  }

  @Test
  void snapshot_restoresCodeAndHaltStatus() throws Exception {
    OutboundWatermark watermark = new OutboundWatermark();
    SnapshotTestFixtures.CoordinatorBundle bundle =
        SnapshotTestFixtures.newCoordinator(
            watermark, (m, c) -> {}, 10_000, 10_000, 2, 5_000);
    SnapshotCoordinator coordinator = bundle.coordinator();
    EngineRegistry registry = bundle.registry();

    try {
      MatchDeltaContext.setOffset(0);
      registry.apply(new CreateSymbol(1, 1L, "ETHUSDT"));
      registry.apply(new PlaceOrder(1, 2L, 1, 1, Side.BID, OrderType.LIMIT, 50, 3));
      registry.apply(new HaltSymbol(1, 3L));
      MatchDeltaContext.clear();

      coordinator.bootstrapFromStore();
      coordinator.start();
      coordinator.onMatched(0L, System.currentTimeMillis());
      coordinator.requestSnapshot();
      watermark.advanceTo(0L);

      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
      while (bundle.snapshotStore().size() == 0 && System.nanoTime() < deadline) {
        Thread.sleep(10);
      }
      assertEquals(1, bundle.snapshotStore().size());

      EngineRegistry restored = new EngineRegistry(SystemTimeSource.INSTANCE);
      assertEquals(
          0L,
          new SnapshotRestorer(bundle.snapshotStore())
              .restoreLatest(restored, "match_event", 0)
              .orElseThrow());
      assertEquals("ETHUSDT", restored.symbols().get(1).code());
      assertEquals(SymbolStatus.HALTED, restored.symbols().get(1).status());
      assertEquals(1, restored.get(1).book().orderCount());
      assertEquals(50, restored.get(1).book().bestBid());

      CommandResult.Place blocked =
          assertInstanceOf(
              CommandResult.Place.class,
              restored
                  .apply(new PlaceOrder(1, 9L, 99, 1, Side.ASK, OrderType.LIMIT, 51, 1))
                  .get(0));
      assertEquals(RejectReason.SYMBOL_NOT_TRADABLE, blocked.rejectReason());
    } finally {
      coordinator.close();
    }
  }
}
