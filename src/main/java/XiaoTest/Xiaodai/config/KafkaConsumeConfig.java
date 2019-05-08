package XiaoTest.Xiaodai.config;

import java.util.HashMap;
import java.util.Map;

import javax.jdo.annotations.Value;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties.Listener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

@Configuration
@EnableKafka
public class KafkaConsumeConfig {

	@Bean("partition-test")
	public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactorySo() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactoryPartition());
		factory.setConcurrency(3);
		factory.setBatchListener(true);
		factory.getContainerProperties().setPollTimeout(5000);
		
		return factory;
	}
	
	public ConsumerFactory<String, String> consumerFactoryPartition() {
		return new DefaultKafkaConsumerFactory<>(consumerConfigsPartition());
	}
	
	public Map<String, Object> consumerConfigsPartition() {
		Map<String, Object> propsMap = new HashMap<>();
		propsMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.10.128:9092");
		propsMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		propsMap.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
		propsMap.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
		propsMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		propsMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		propsMap.put(ConsumerConfig.GROUP_ID_CONFIG, "partition-test-group");
		propsMap.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 600);
		propsMap.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 8 * 1024 * 1024);
		propsMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		return propsMap;
	}
	
	@Bean
	public Listener listener() {
	    return new Listener();
	}
}
