package com.kai.matchengine.snapshot;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.sql.DataSource;

/**
 * JDBC 全量 compaction 单事务：snapshot save + retain + batch prune。
 *
 * <p>避免 save 成功但 prune 失败导致的多余 batch 与恢复歧义。
 */
public final class JdbcSnapshotCompactionWriter {

  private final DataSource dataSource;

  public JdbcSnapshotCompactionWriter(DataSource dataSource) {
    this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
  }

  public long persistFullCompaction(
      EngineSnapshot snapshot,
      List<SnapshotStore.Section> sections,
      int retainCount,
      long oldBaseSnapshotId,
      String matchTopic,
      int matchPartition,
      long throughOffset) {
    try (var conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      try {
        long newId = insertSnapshot(conn, snapshot, sections);
        retainSnapshots(conn, matchTopic, matchPartition, retainCount);
        if (oldBaseSnapshotId > 0) {
          pruneDeltaBatches(
              conn, oldBaseSnapshotId, matchTopic, matchPartition, throughOffset);
        }
        conn.commit();
        return newId;
      } catch (SQLException e) {
        conn.rollback();
        throw e;
      } finally {
        conn.setAutoCommit(true);
      }
    } catch (SQLException e) {
      throw new IllegalStateException("full compaction persist failed", e);
    }
  }

  private static long insertSnapshot(
      java.sql.Connection conn, EngineSnapshot snapshot, List<SnapshotStore.Section> sections)
      throws SQLException {
    long id;
    try (PreparedStatement ps =
        conn.prepareStatement(
            """
            INSERT INTO engine_snapshot(schema_version, topic, partition_id, last_fully_processed_offset)
            VALUES (?,?,?,?)
            """,
            Statement.RETURN_GENERATED_KEYS)) {
      ps.setInt(1, snapshot.schemaVersion());
      ps.setString(2, snapshot.matchTopic());
      ps.setInt(3, snapshot.matchPartition());
      ps.setLong(4, snapshot.lastFullyProcessedOffset());
      ps.executeUpdate();
      try (ResultSet keys = ps.getGeneratedKeys()) {
        if (!keys.next()) {
          throw new IllegalStateException("no generated snapshot id");
        }
        id = keys.getLong(1);
      }
    }
    if (!sections.isEmpty()) {
      try (PreparedStatement ps =
          conn.prepareStatement(
              """
              INSERT INTO engine_snapshot_section(snapshot_id, section_type, section_key, encoding, payload)
              VALUES (?,?,?,?,?)
              """)) {
        for (SnapshotStore.Section section : sections) {
          ps.setLong(1, id);
          ps.setString(2, section.sectionType());
          ps.setString(3, section.sectionKey());
          ps.setString(4, section.encoding());
          ps.setBytes(5, section.payload());
          ps.addBatch();
        }
        ps.executeBatch();
      }
    }
    return id;
  }

  private static void retainSnapshots(
      java.sql.Connection conn, String matchTopic, int matchPartition, int keep)
      throws SQLException {
    List<Long> ids = new ArrayList<>();
    try (PreparedStatement ps =
        conn.prepareStatement(
            """
            SELECT id FROM engine_snapshot
            WHERE topic=? AND partition_id=?
            ORDER BY last_fully_processed_offset DESC
            """)) {
      ps.setString(1, matchTopic);
      ps.setInt(2, matchPartition);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          ids.add(rs.getLong(1));
        }
      }
    }
    if (ids.size() <= keep) {
      return;
    }
    try (PreparedStatement delSec =
            conn.prepareStatement("DELETE FROM engine_snapshot_section WHERE snapshot_id=?");
        PreparedStatement delMeta =
            conn.prepareStatement("DELETE FROM engine_snapshot WHERE id=?")) {
      for (int i = keep; i < ids.size(); i++) {
        long id = ids.get(i);
        delSec.setLong(1, id);
        delSec.addBatch();
        delMeta.setLong(1, id);
        delMeta.addBatch();
      }
      delSec.executeBatch();
      delMeta.executeBatch();
    }
  }

  private static void pruneDeltaBatches(
      java.sql.Connection conn,
      long baseSnapshotId,
      String matchTopic,
      int matchPartition,
      long throughOffset)
      throws SQLException {
    List<Long> ids = new ArrayList<>();
    try (PreparedStatement ps =
        conn.prepareStatement(
            """
            SELECT id FROM engine_delta_batch
            WHERE base_snapshot_id=? AND topic=? AND partition_id=? AND to_offset<=?
            """)) {
      ps.setLong(1, baseSnapshotId);
      ps.setString(2, matchTopic);
      ps.setInt(3, matchPartition);
      ps.setLong(4, throughOffset);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          ids.add(rs.getLong(1));
        }
      }
    }
    if (ids.isEmpty()) {
      return;
    }
    try (PreparedStatement delSec =
            conn.prepareStatement("DELETE FROM engine_delta_batch_section WHERE batch_id=?");
        PreparedStatement delBatch =
            conn.prepareStatement("DELETE FROM engine_delta_batch WHERE id=?")) {
      for (long id : ids) {
        delSec.setLong(1, id);
        delSec.addBatch();
        delBatch.setLong(1, id);
        delBatch.addBatch();
      }
      delSec.executeBatch();
      delBatch.executeBatch();
    }
  }
}
