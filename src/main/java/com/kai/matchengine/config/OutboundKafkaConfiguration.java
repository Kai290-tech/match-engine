package com.kai.matchengine.config;

import com.kai.matchengine.ingress.FatalFailureHandler;
import com.kai.matchengine.outbound.AsyncRecordSender;
import com.kai.matchengine.outbound.BackpressureMonitor;
import com.kai.matchengine.outbound.KafkaAsyncRecordSender;
import com.kai.matchengine.outbound.LoggingBackpressureMonitor;
import com.kai.matchengine.outbound.OutboundEventEncoder;
import com.kai.matchengine.outbound.OutboundJsonEncoder;
import com.kai.matchengine.outbound.OutboundKafkaFactory;
import com.kai.matchengine.outbound.OutboundPublisher;
import com.kai.matchengine.outbound.OutboundTopics;
import com.kai.matchengine.outbound.OutboundWatermark;
import com.kai.matchengine.outbound.SbeOutboundEncoder;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 出站 Kafka 装配（{@code outbound.enabled=true} 时生效）。
 *
 * <p>不在此处 {@code start()} Publisher；由 {@link MatchEnginePipelineConfiguration} 在 seed
 * watermark 之后启动。
 */
@Configuration
@EnableConfigurationProperties(OutboundProperties.class)
@ConditionalOnProperty(prefix = "outbound", name = "enabled", havingValue = "true")
public class OutboundKafkaConfiguration {

  @Bean(destroyMethod = "close")
  Producer<byte[], byte[]> outboundKafkaProducer(OutboundProperties props) {
    return OutboundKafkaFactory.createProducer(props.getKafka().getBootstrapServers());
  }

  @Bean
  OutboundTopics outboundTopics(OutboundProperties props) {
    return props.toTopics();
  }

  @Bean
  OutboundWatermark outboundWatermark() {
    return new OutboundWatermark();
  }

  @Bean(destroyMethod = "close")
  AsyncRecordSender outboundAsyncRecordSender(Producer<byte[], byte[]> outboundKafkaProducer) {
    return new KafkaAsyncRecordSender(outboundKafkaProducer);
  }

  @Bean
  OutboundEventEncoder outboundEventEncoder(OutboundProperties props) {
    if ("json".equalsIgnoreCase(props.getFormat())) {
      return new OutboundJsonEncoder();
    }
    return new SbeOutboundEncoder();
  }

  @Bean
  BackpressureMonitor outboundBackpressureMonitor(OutboundProperties props) {
    OutboundProperties.Backpressure bp = props.getBackpressure();
    return new LoggingBackpressureMonitor(bp.getWarnRatio(), bp.getLogThrottleMs());
  }

  @Bean(destroyMethod = "close")
  OutboundPublisher outboundPublisher(
      OutboundProperties props,
      OutboundWatermark outboundWatermark,
      AsyncRecordSender outboundAsyncRecordSender,
      OutboundTopics outboundTopics,
      OutboundEventEncoder outboundEventEncoder,
      BackpressureMonitor outboundBackpressureMonitor) {
    return new OutboundPublisher(
        props.getQueueCapacity(),
        outboundWatermark,
        outboundAsyncRecordSender,
        outboundTopics,
        outboundEventEncoder,
        outboundBackpressureMonitor,
        FatalFailureHandler.SYSTEM_EXIT,
        props.getSendTimeoutMs());
  }
}
