package com.kai.matchengine.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kai.matchengine.event.inbound.OrderType;
import com.kai.matchengine.event.inbound.PlaceOrder;
import com.kai.matchengine.match.EngineRegistry;
import com.kai.matchengine.match.SystemTimeSource;
import com.kai.matchengine.orderbook.Side;
import java.util.List;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdbcSnapshotStoreTest {

  private JdbcSnapshotStore store;

  @BeforeEach
  void setUp() throws Exception {
    JdbcDataSource ds = new JdbcDataSource();
    ds.setURL("jdbc:h2:mem:snap_" + System.nanoTime() + ";MODE=MySQL;DB_CLOSE_DELAY=-1");
    ds.setUser("sa");
    ds.setPassword("");
    store = new JdbcSnapshotStore(ds);
    store.initSchema();
  }

  @Test
  void saveLoadRetain_roundTrip() {
    EngineRegistry registry = new EngineRegistry(SystemTimeSource.INSTANCE);
    registry.register(1);
    registry.apply(new PlaceOrder(1, 9L, 1, 1, Side.ASK, OrderType.LIMIT, 200, 3));
    SnapshotPreparer preparer = new SnapshotPreparer(registry);
    SnapshotCodec codec = new SnapshotCodec();

    EngineSnapshot s0 = preparer.prepare("match_event", 0, 0L, 1L);
    EngineSnapshot s1 = preparer.prepare("match_event", 0, 1L, 2L);
    EngineSnapshot s2 = preparer.prepare("match_event", 0, 2L, 3L);

    for (EngineSnapshot snap : List.of(s0, s1, s2)) {
      store.save(
          snap,
          List.of(
              new SnapshotStore.Section(
                  SnapshotCodec.SECTION_ENGINE_META,
                  "",
                  SnapshotCodec.ENCODING_JSON_GZIP,
                  codec.encodeMeta(snap)),
              new SnapshotStore.Section(
                  SnapshotCodec.SECTION_MATCH_SYMBOL,
                  "1",
                  SnapshotCodec.ENCODING_JSON_GZIP,
                  codec.encodeSymbol(snap.symbols().getFirst()))));
      store.retainLatest("match_event", 0, 2);
    }

    var latest = store.loadLatest("match_event", 0).orElseThrow();
    assertEquals(2L, latest.lastFullyProcessedOffset());
    assertEquals(2, latest.sections().size());

    EngineRegistry restored = new EngineRegistry(SystemTimeSource.INSTANCE);
    assertTrue(new SnapshotRestorer(store).restoreLatest(restored, "match_event", 0).isPresent());
    assertEquals(200, restored.get(1).book().bestAsk());
    assertEquals(3L, restored.get(1).book().get(1).qty());
  }
}
