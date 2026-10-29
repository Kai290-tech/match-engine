package com.kai.matchengine.snapshot;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.sql.DataSource;

/** JDBC 快照存储（MySQL / H2 兼容 SQL）。save / retain 均在显式事务内。 */
public final class JdbcSnapshotStore implements SnapshotStore {

  private final DataSource dataSource;

  public JdbcSnapshotStore(DataSource dataSource) {
    this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
  }

  /** 建表（启动时调用）。 */
  public void initSchema() throws SQLException {
    try (var conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      st.execute(
          """
          CREATE TABLE IF NOT EXISTS engine_snapshot (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            schema_version SMALLINT NOT NULL,
            topic VARCHAR(128) NOT NULL,
            partition_id INT NOT NULL,
            last_fully_processed_offset BIGINT NOT NULL,
            created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
            UNIQUE (topic, partition_id, last_fully_processed_offset)
          )
          """);
      st.execute(
          """
          CREATE TABLE IF NOT EXISTS engine_snapshot_section (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            snapshot_id BIGINT NOT NULL,
            section_type VARCHAR(32) NOT NULL,
            section_key VARCHAR(64) NOT NULL DEFAULT '',
            encoding VARCHAR(16) NOT NULL,
            payload VARBINARY(16777215) NOT NULL,
            UNIQUE (snapshot_id, section_type, section_key)
          )
          """);
    }
  }

  @Override
  public long save(EngineSnapshot snapshot, List<Section> sections) {
    try (var conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      try {
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
            for (Section section : sections) {
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
        conn.commit();
        return id;
      } catch (SQLException e) {
        conn.rollback();
        throw e;
      } finally {
        conn.setAutoCommit(true);
      }
    } catch (SQLException e) {
      throw new IllegalStateException("snapshot save failed", e);
    }
  }

  @Override
  public Optional<StoredSnapshot> loadLatest(String matchTopic, int matchPartition) {
    try (var conn = dataSource.getConnection()) {
      Long id = null;
      int schemaVersion = 0;
      long offset = -1L;
      try (PreparedStatement ps =
          conn.prepareStatement(
              """
              SELECT id, schema_version, last_fully_processed_offset
              FROM engine_snapshot
              WHERE topic=? AND partition_id=?
              ORDER BY last_fully_processed_offset DESC
              LIMIT 1
              """)) {
        ps.setString(1, matchTopic);
        ps.setInt(2, matchPartition);
        try (ResultSet rs = ps.executeQuery()) {
          if (!rs.next()) {
            return Optional.empty();
          }
          id = rs.getLong(1);
          schemaVersion = rs.getInt(2);
          offset = rs.getLong(3);
        }
      }
      List<Section> sections = new ArrayList<>();
      try (PreparedStatement ps =
          conn.prepareStatement(
              """
              SELECT section_type, section_key, encoding, payload
              FROM engine_snapshot_section WHERE snapshot_id=?
              """)) {
        ps.setLong(1, id);
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            sections.add(
                new Section(rs.getString(1), rs.getString(2), rs.getString(3), rs.getBytes(4)));
          }
        }
      }
      StoredSnapshot stored =
          new StoredSnapshot(id, schemaVersion, matchTopic, matchPartition, offset, sections);
      SnapshotStoreValidator.validateStoredSnapshot(stored);
      return Optional.of(stored);
    } catch (SQLException e) {
      throw new IllegalStateException("snapshot load failed", e);
    }
  }

  @Override
  public void retainLatest(String matchTopic, int matchPartition, int keep) {
    try (var conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      try {
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
        if (ids.size() > keep) {
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
        conn.commit();
      } catch (SQLException e) {
        conn.rollback();
        throw e;
      } finally {
        conn.setAutoCommit(true);
      }
    } catch (SQLException e) {
      throw new IllegalStateException("snapshot retain failed", e);
    }
  }
}
