package com.kai.matchengine.snapshot.delta;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.sql.DataSource;

/** JDBC 增量 batch 存储。 */
public final class JdbcBookDeltaBatchStore implements BookDeltaBatchStore {

  private final DataSource dataSource;

  public JdbcBookDeltaBatchStore(DataSource dataSource) {
    this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
  }

  public void initSchema() throws SQLException {
    try (var conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      st.execute(
          """
          CREATE TABLE IF NOT EXISTS engine_delta_batch (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            base_snapshot_id BIGINT NOT NULL,
            topic VARCHAR(128) NOT NULL,
            partition_id INT NOT NULL,
            from_offset BIGINT NOT NULL,
            to_offset BIGINT NOT NULL,
            delta_count INT NOT NULL,
            created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
            UNIQUE (base_snapshot_id, from_offset)
          )
          """);
      st.execute(
          """
          CREATE TABLE IF NOT EXISTS engine_delta_batch_section (
            batch_id BIGINT NOT NULL,
            symbol_id INT NOT NULL,
            encoding VARCHAR(16) NOT NULL,
            payload MEDIUMBLOB NOT NULL,
            PRIMARY KEY (batch_id, symbol_id)
          )
          """);
    }
  }

  @Override
  public long save(
      long baseSnapshotId,
      String matchTopic,
      int matchPartition,
      long fromOffset,
      long toOffset,
      int deltaCount,
      List<Section> sections) {
    try (var conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      try {
        long id;
        try (PreparedStatement ps =
            conn.prepareStatement(
                """
                INSERT INTO engine_delta_batch(base_snapshot_id, topic, partition_id, from_offset, to_offset, delta_count)
                VALUES (?,?,?,?,?,?)
                """,
                Statement.RETURN_GENERATED_KEYS)) {
          ps.setLong(1, baseSnapshotId);
          ps.setString(2, matchTopic);
          ps.setInt(3, matchPartition);
          ps.setLong(4, fromOffset);
          ps.setLong(5, toOffset);
          ps.setInt(6, deltaCount);
          ps.executeUpdate();
          try (ResultSet keys = ps.getGeneratedKeys()) {
            if (!keys.next()) {
              throw new IllegalStateException("no generated batch id");
            }
            id = keys.getLong(1);
          }
        }
        if (!sections.isEmpty()) {
          try (PreparedStatement ps =
              conn.prepareStatement(
                  """
                  INSERT INTO engine_delta_batch_section(batch_id, symbol_id, encoding, payload)
                  VALUES (?,?,?,?)
                  """)) {
            for (Section section : sections) {
              ps.setLong(1, id);
              ps.setInt(2, section.symbolId());
              ps.setString(3, BookDeltaCodec.ENCODING_JSON_GZIP);
              ps.setBytes(4, section.payload());
              ps.addBatch();
            }
            ps.executeBatch();
          }
        }
        conn.commit();
        return id;
      } catch (SQLException e) {
        conn.rollback();
        throw e;
      } finally {
        conn.setAutoCommit(true);
      }
    } catch (SQLException e) {
      throw new IllegalStateException("delta batch save failed", e);
    }
  }

  @Override
  public List<StoredBatch> loadChain(long baseSnapshotId, String matchTopic, int matchPartition) {
    try (var conn = dataSource.getConnection()) {
      List<StoredBatch> batches = new ArrayList<>();
      try (PreparedStatement ps =
          conn.prepareStatement(
              """
              SELECT id, from_offset, to_offset, delta_count
              FROM engine_delta_batch
              WHERE base_snapshot_id=? AND topic=? AND partition_id=?
              ORDER BY from_offset
              """)) {
        ps.setLong(1, baseSnapshotId);
        ps.setString(2, matchTopic);
        ps.setInt(3, matchPartition);
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            long batchId = rs.getLong(1);
            long from = rs.getLong(2);
            long to = rs.getLong(3);
            int count = rs.getInt(4);
            batches.add(
                new StoredBatch(
                    batchId,
                    baseSnapshotId,
                    matchTopic,
                    matchPartition,
                    from,
                    to,
                    count,
                    loadSections(conn, batchId)));
          }
        }
      }
      return batches;
    } catch (SQLException e) {
      throw new IllegalStateException("delta batch load failed", e);
    }
  }

  @Override
  public void pruneThrough(
      long baseSnapshotId, String matchTopic, int matchPartition, long throughOffset) {
    try (var conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      try {
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
        if (!ids.isEmpty()) {
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
        conn.commit();
      } catch (SQLException e) {
        conn.rollback();
        throw e;
      } finally {
        conn.setAutoCommit(true);
      }
    } catch (SQLException e) {
      throw new IllegalStateException("delta batch prune failed", e);
    }
  }

  private static List<Section> loadSections(java.sql.Connection conn, long batchId)
      throws SQLException {
    List<Section> sections = new ArrayList<>();
    try (PreparedStatement ps =
        conn.prepareStatement(
            "SELECT symbol_id, payload FROM engine_delta_batch_section WHERE batch_id=?")) {
      ps.setLong(1, batchId);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          sections.add(new Section(rs.getInt(1), rs.getBytes(2)));
        }
      }
    }
    return sections;
  }
}
