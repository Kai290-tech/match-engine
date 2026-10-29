package com.kai.matchengine.match;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class RecentOrderIdWindowTest {

  @Test
  void remembersAndEvictsFifo() {
    RecentOrderIdWindow w = new RecentOrderIdWindow(3);
    assertTrue(w.remember(1));
    assertTrue(w.remember(2));
    assertTrue(w.remember(3));
    assertFalse(w.remember(2));
    assertTrue(w.remember(4)); // evict 1
    assertFalse(w.contains(1));
    assertTrue(w.contains(2));
    assertEquals(List.of(2L, 3L, 4L), w.snapshotOldestFirst());
  }

  @Test
  void restoreOldestFirst() {
    RecentOrderIdWindow w = new RecentOrderIdWindow(10_000);
    w.restoreOldestFirst(List.of(10L, 20L, 30L));
    assertTrue(w.contains(10));
    assertEquals(List.of(10L, 20L, 30L), w.snapshotOldestFirst());
  }
}
