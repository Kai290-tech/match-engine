package com.kai.matchengine.config;

import com.kai.matchengine.ingress.FatalFailureHandler;
import com.kai.matchengine.ingress.IngressValidator;
import com.kai.matchengine.ingress.KafkaIngressRunner;
import com.kai.matchengine.ingress.MatchEventProcessor;
import com.kai.matchengine.ingress.OffsetStore;
import com.kai.matchengine.ingress.RejectedOffsetHandler;
import com.kai.matchengine.ingress.SbeMatchEventCodec;
import com.kai.matchengine.ingress.disruptor.IngressDisruptorPublisher;
import com.kai.matchengine.ingress.disruptor.MatchIngressDisruptor;
import com.kai.matchengine.match.EngineRegistry;
import com.kai.matchengine.match.SystemTimeSource;
import com.kai.matchengine.outbound.MatchPipelineOutboundListener;
import com.kai.matchengine.outbound.OutboundPublisher;
import com.kai.matchengine.outbound.OutboundWatermark;
import com.kai.matchengine.snapshot.InMemorySnapshotStore;
import com.kai.matchengine.snapshot.JdbcSnapshotCompactionWriter;
import com.kai.matchengine.snapshot.JdbcSnapshotStore;
import com.kai.matchengine.snapshot.SnapshotCoordinator;
import com.kai.matchengine.snapshot.SnapshotOffsetStore;
import com.kai.matchengine.snapshot.SnapshotRestorer;
import com.kai.matchengine.snapshot.SnapshotStore;
import com.kai.matchengine.snapshot.delta.BookDeltaBatchStore;
import com.kai.matchengine.snapshot.delta.BookDeltaLog;
import com.kai.matchengine.snapshot.delta.BookDeltaRecorder;
import com.kai.matchengine.snapshot.delta.InMemoryBookDeltaBatchStore;
import com.kai.matchengine.snapshot.delta.JdbcBookDeltaBatchStore;
import com.kai.matchengine.snapshot.delta.SnapshotMaterializer;
import java.time.Duration;
import java.util.Optional;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 完整撮合管道：恢复快照 → seed watermark → Disruptor + Outbound + Snapshot + Kafka Ingress。
 *
 * <p>需同时 {@code match-engine.pipeline-enabled=true} 且 {@code outbound.enabled=true}。
 */
@Configuration
@EnableConfigurationProperties({IngressProperties.class, SnapshotProperties.class, MatchEngineProperties.class})
@ConditionalOnProperty(prefix = "match-engine", name = "pipeline-enabled", havingValue = "true")
@ConditionalOnProperty(prefix = "outbound", name = "enabled", havingValue = "true")
public class MatchEnginePipelineConfiguration {

  private static final Logger log = LoggerFactory.getLogger(MatchEnginePipelineConfiguration.class);

  @Bean
  BookDeltaLog bookDeltaLog(SnapshotProperties snapProps) {
    return new BookDeltaLog(
        snapProps.getDeltaLogRingCapacity(), snapProps.getDeltaLogSpillCapacity());
  }

  @Bean
  BookDeltaRecorder bookDeltaRecorder(BookDeltaLog bookDeltaLog) {
    return new BookDeltaRecorder(bookDeltaLog);
  }

  @Bean
  SnapshotMaterializer snapshotMaterializer() {
    return new SnapshotMaterializer();
  }

  @Bean
  BookDeltaBatchStore bookDeltaBatchStore(
      SnapshotProperties props,
      org.springframework.beans.factory.ObjectProvider<DataSource> dataSources)
      throws Exception {
    if ("jdbc".equalsIgnoreCase(props.getStore())) {
      DataSource ds = dataSources.getIfAvailable();
      if (ds == null) {
        throw new IllegalStateException("snapshot.store=jdbc requires a DataSource bean");
      }
      JdbcBookDeltaBatchStore jdbc = new JdbcBookDeltaBatchStore(ds);
      jdbc.initSchema();
      return jdbc;
    }
    return new InMemoryBookDeltaBatchStore();
  }

  @Bean
  EngineRegistry engineRegistry(
      MatchEngineProperties matchEngineProperties, BookDeltaRecorder bookDeltaRecorder) {
    return new EngineRegistry(
        SystemTimeSource.INSTANCE,
        matchEngineProperties.getPool().getOrderNodeCapacityPerSymbol(),
        bookDeltaRecorder);
  }

  @Bean
  SnapshotStore snapshotStore(
      SnapshotProperties props,
      org.springframework.beans.factory.ObjectProvider<DataSource> dataSources)
      throws Exception {
    if ("jdbc".equalsIgnoreCase(props.getStore())) {
      DataSource ds = dataSources.getIfAvailable();
      if (ds == null) {
        throw new IllegalStateException("snapshot.store=jdbc requires a DataSource bean");
      }
      JdbcSnapshotStore jdbc = new JdbcSnapshotStore(ds);
      jdbc.initSchema();
      return jdbc;
    }
    return new InMemorySnapshotStore();
  }

  @Bean
  OffsetStore offsetStore(
      SnapshotStore snapshotStore,
      BookDeltaBatchStore batchStore,
      IngressProperties ingress) {
    return new SnapshotOffsetStore(
        snapshotStore, batchStore, ingress.getKafka().getStartOffset());
  }

  @Bean
  @org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(
      prefix = "snapshot",
      name = "store",
      havingValue = "jdbc")
  JdbcSnapshotCompactionWriter jdbcSnapshotCompactionWriter(DataSource dataSource) {
    return new JdbcSnapshotCompactionWriter(dataSource);
  }

  @Bean(destroyMethod = "close")
  SnapshotCoordinator snapshotCoordinator(
      EngineRegistry registry,
      SnapshotStore store,
      BookDeltaBatchStore batchStore,
      BookDeltaLog bookDeltaLog,
      SnapshotMaterializer materializer,
      OutboundWatermark watermark,
      IngressProperties ingress,
      SnapshotProperties snapProps,
      org.springframework.beans.factory.ObjectProvider<JdbcSnapshotCompactionWriter>
          compactionWriters) {
    return new SnapshotCoordinator(
        registry,
        store,
        batchStore,
        bookDeltaLog,
        materializer,
        watermark,
        FatalFailureHandler.SYSTEM_EXIT,
        ingress.getKafka().getTopic(),
        ingress.getKafka().getPartition(),
        snapProps.getDeltaTriggerOffsetSpan(),
        snapProps.getDeltaTriggerDeltaCount(),
        snapProps.getFullMaxBookFillRatio(),
        snapProps.getRetainCount(),
        snapProps.getPersistAwaitTimeoutMs(),
        compactionWriters.getIfAvailable());
  }

  @Bean(destroyMethod = "close")
  MatchIngressDisruptor matchIngressDisruptor(
      EngineRegistry registry,
      OutboundPublisher outboundPublisher,
      SnapshotCoordinator snapshotCoordinator,
      IngressProperties ingress) {
    MatchPipelineOutboundListener listener =
        new MatchPipelineOutboundListener(
            outboundPublisher,
            snapshotCoordinator,
            ingress.getKafka().getTopic(),
            ingress.getKafka().getPartition(),
            System::currentTimeMillis);
    return new MatchIngressDisruptor(
        registry,
        1024 * 64,
        MatchIngressDisruptor.WaitMode.BLOCKING,
        null,
        listener,
        FatalFailureHandler.SYSTEM_EXIT);
  }

  @Bean
  MatchEventProcessor matchEventProcessor(MatchIngressDisruptor disruptor) {
    IngressDisruptorPublisher publisher = disruptor.publisher();
    RejectedOffsetHandler rejectedOffsets = publisher::onRejected;
    return new MatchEventProcessor(
        new SbeMatchEventCodec(),
        new IngressValidator(),
        publisher,
        FatalFailureHandler.SYSTEM_EXIT,
        rejectedOffsets);
  }

  @Bean(destroyMethod = "close")
  KafkaIngressRunner kafkaIngressRunner(
      IngressProperties ingress, OffsetStore offsetStore, MatchEventProcessor processor) {
    // 每次启动新 groupId：不继承旧 consumer group 的 commit，避免超前位点导致漏消费；
    // 真正起点仍是快照 seek。同机多活会各自订阅全量，生产应单活。
    String groupId = "match-engine-" + java.util.UUID.randomUUID();
    log.info("Kafka ingress ephemeral groupId={}", groupId);
    return new KafkaIngressRunner(
        ingress.getKafka().getBootstrapServers(),
        ingress.getKafka().getTopic(),
        ingress.getKafka().getPartition(),
        groupId,
        offsetStore,
        processor,
        Duration.ofMillis(500));
  }

  @Bean
  ApplicationRunner matchEnginePipelineStarter(
      EngineRegistry registry,
      SnapshotStore snapshotStore,
      BookDeltaBatchStore batchStore,
      OffsetStore offsetStore,
      OutboundWatermark watermark,
      OutboundPublisher outboundPublisher,
      SnapshotCoordinator snapshotCoordinator,
      MatchIngressDisruptor disruptor,
      KafkaIngressRunner ingressRunner,
      IngressProperties ingress) {
    return (ApplicationArguments args) -> {
      String topic = ingress.getKafka().getTopic();
      int partition = ingress.getKafka().getPartition();

      Optional<Long> restored =
          new SnapshotRestorer(snapshotStore, batchStore).restoreLatest(registry, topic, partition);
      snapshotCoordinator.bootstrapFromStore();
      long startOffset = offsetStore.loadStartingOffset(topic, partition);
      watermark.seedBefore(startOffset);
      log.info(
          "pipeline starting topic={} partition={} startOffset={} restoredSnapshot={} symbols={}",
          topic,
          partition,
          startOffset,
          restored.map(Object::toString).orElse("none"),
          registry.symbolIds().size());

      snapshotCoordinator.start();
      outboundPublisher.start();
      disruptor.start();

      Thread ingressThread = new Thread(ingressRunner, "kafka-ingress");
      ingressThread.setDaemon(false);
      ingressThread.start();
    };
  }
}
