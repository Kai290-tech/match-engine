package com.kai.matchengine.snapshot;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** 快照 load 完整性校验。 */
public final class SnapshotStoreValidator {

  private SnapshotStoreValidator() {}

  public static void validateStoredSnapshot(SnapshotStore.StoredSnapshot stored) {
    Objects.requireNonNull(stored, "stored");
    SnapshotCodec codec = new SnapshotCodec();
    byte[] metaPayload = null;
    Set<Integer> metaSymbolIds = Set.of();
    Set<Integer> sectionSymbolIds = new HashSet<>();

    for (SnapshotStore.Section section : stored.sections()) {
      if (SnapshotCodec.SECTION_ENGINE_META.equals(section.sectionType())) {
        if (metaPayload != null) {
          throw new IllegalStateException(
              "duplicate ENGINE_META snapshotId=" + stored.id());
        }
        metaPayload = section.payload();
        metaSymbolIds = new HashSet<>(codec.decodeMetaSymbolIds(metaPayload));
      } else if (SnapshotCodec.SECTION_MATCH_SYMBOL.equals(section.sectionType())) {
        try {
          sectionSymbolIds.add(Integer.parseInt(section.sectionKey()));
        } catch (NumberFormatException e) {
          throw new IllegalStateException(
              "invalid MATCH_SYMBOL key=" + section.sectionKey() + " snapshotId=" + stored.id());
        }
      }
    }

    if (metaPayload == null) {
      throw new IllegalStateException("missing ENGINE_META snapshotId=" + stored.id());
    }
    if (!metaSymbolIds.equals(sectionSymbolIds)) {
      throw new IllegalStateException(
          "snapshot section mismatch snapshotId="
              + stored.id()
              + " meta="
              + metaSymbolIds
              + " sections="
              + sectionSymbolIds);
    }

    for (SnapshotStore.Section section : stored.sections()) {
      if (!SnapshotCodec.SECTION_MATCH_SYMBOL.equals(section.sectionType())) {
        continue;
      }
      codec.decodeSymbol(section.payload());
    }
  }
}
