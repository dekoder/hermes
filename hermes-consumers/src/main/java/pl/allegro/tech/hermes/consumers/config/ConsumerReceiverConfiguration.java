package pl.allegro.tech.hermes.consumers.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.kafka.KafkaNamesMapper;
import pl.allegro.tech.hermes.common.message.wrapper.CompositeMessageContentWrapper;
import pl.allegro.tech.hermes.common.metric.HermesMetrics;
import pl.allegro.tech.hermes.consumers.consumer.offset.ConsumerPartitionAssignmentState;
import pl.allegro.tech.hermes.consumers.consumer.offset.OffsetQueue;
import pl.allegro.tech.hermes.consumers.consumer.receiver.ReceiverFactory;
import pl.allegro.tech.hermes.consumers.consumer.receiver.kafka.BasicMessageContentReaderFactory;
import pl.allegro.tech.hermes.consumers.consumer.receiver.kafka.KafkaHeaderExtractor;
import pl.allegro.tech.hermes.consumers.consumer.receiver.kafka.KafkaConsumerRecordToMessageConverterFactory;
import pl.allegro.tech.hermes.consumers.consumer.receiver.kafka.KafkaMessageReceiverFactory;
import pl.allegro.tech.hermes.consumers.consumer.receiver.kafka.MessageContentReaderFactory;
import pl.allegro.tech.hermes.consumers.consumer.resources.ResourcesGuard;
import pl.allegro.tech.hermes.domain.filtering.chain.FilterChainFactory;
import pl.allegro.tech.hermes.tracker.consumers.Trackers;

import java.time.Clock;
import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL;
import static org.apache.kafka.clients.CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_ADMIN_REQUEST_TIMEOUT_MS;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_AUTHORIZATION_ENABLED;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_AUTHORIZATION_MECHANISM;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_AUTHORIZATION_PASSWORD;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_AUTHORIZATION_PROTOCOL;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_AUTHORIZATION_USERNAME;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_BROKER_LIST;

@Configuration
public class ConsumerReceiverConfiguration {

    @Bean
    public ReceiverFactory kafkaMessageReceiverFactory(ConfigFactory configs,
                                                       KafkaConsumerRecordToMessageConverterFactory messageConverterFactory,
                                                       HermesMetrics hermesMetrics,
                                                       OffsetQueue offsetQueue,
                                                       KafkaNamesMapper kafkaNamesMapper,
                                                       FilterChainFactory filterChainFactory,
                                                       Trackers trackers,
                                                       ConsumerPartitionAssignmentState consumerPartitionAssignmentState,
                                                       ResourcesGuard resourcesGuard) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, configs.getStringProperty(KAFKA_BROKER_LIST));
        props.put(SECURITY_PROTOCOL_CONFIG, DEFAULT_SECURITY_PROTOCOL);
        props.put(REQUEST_TIMEOUT_MS_CONFIG, configs.getIntProperty(KAFKA_ADMIN_REQUEST_TIMEOUT_MS));
        if (configs.getBooleanProperty(KAFKA_AUTHORIZATION_ENABLED)) {
            props.put(SASL_MECHANISM, configs.getStringProperty(KAFKA_AUTHORIZATION_MECHANISM));
            props.put(SECURITY_PROTOCOL_CONFIG, configs.getStringProperty(KAFKA_AUTHORIZATION_PROTOCOL));
            props.put(SASL_JAAS_CONFIG,
                    "org.apache.kafka.common.security.plain.PlainLoginModule required\n"
                            + "username=\"" + configs.getStringProperty(KAFKA_AUTHORIZATION_USERNAME) + "\"\n"
                            + "password=\"" + configs.getStringProperty(KAFKA_AUTHORIZATION_PASSWORD) + "\";"
            );
        }
        AdminClient adminClient = AdminClient.create(props);
        return new KafkaMessageReceiverFactory(
                configs,
                messageConverterFactory,
                hermesMetrics,
                offsetQueue,
                kafkaNamesMapper,
                filterChainFactory,
                trackers,
                consumerPartitionAssignmentState,
                resourcesGuard,
                adminClient
        );
    }

    @Bean
    public KafkaConsumerRecordToMessageConverterFactory kafkaMessageConverterFactory(MessageContentReaderFactory messageContentReaderFactory,
                                                                                     Clock clock) {
        return new KafkaConsumerRecordToMessageConverterFactory(messageContentReaderFactory, clock);
    }

    @Bean
    public MessageContentReaderFactory messageContentReaderFactory(CompositeMessageContentWrapper compositeMessageContentWrapper,
                                                                   KafkaHeaderExtractor kafkaHeaderExtractor) {
        return new BasicMessageContentReaderFactory(compositeMessageContentWrapper, kafkaHeaderExtractor);
    }

    @Bean
    public KafkaHeaderExtractor kafkaHeaderExtractor(ConfigFactory configFactory) {
        return new KafkaHeaderExtractor(configFactory);
    }
}
