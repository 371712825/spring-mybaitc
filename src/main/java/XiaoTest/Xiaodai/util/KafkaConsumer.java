package XiaoTest.Xiaodai.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

@Component
public class KafkaConsumer implements Runnable {

	private final static Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);
	
	private ConsumerConfig consumerConfig;
	private static String topic="test";
	Properties props;
	final int a_numThreads = 3;
	
	public KafkaConsumer() {
		props = new Properties();    
        props.put("zookeeper.connect", "192.168.10.128:2181");  
        props.put("zookeeper.connectiontimeout.ms", "30000");    
        props.put("group.id", "test");  
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000"); 
        props.put("auto.offset.reset", "smallest");
        consumerConfig = new ConsumerConfig(props);
	}
	
	@KafkaListener(topics = { "test" },containerFactory = "partition-test")
	public void listen(List<ConsumerRecord<?, ?>> messages) {
		
		logger.info("接受开始");
		for (ConsumerRecord<?,?> consumer : messages) {
			System.out.println(consumer.value().toString());
			System.out.println("1111");
		}
		logger.info("接受完毕");
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
	    topicCountMap.put(topic, new Integer(a_numThreads));
	    ConsumerConfig consumerConfig = new ConsumerConfig(props);
		ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);
	    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
	    List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
	    ExecutorService executor = Executors.newFixedThreadPool(a_numThreads);
	    try {
	    	for (final KafkaStream stream : streams) {
		    	Future<String> future = (Future<String>) executor.submit(new KafkaConsumerThread(stream));
		    	while(future.isDone()) {
		    		logger.info(">>>>>>>>>>>>>>>>wait");
		    	};
		    	logger.info(future.get());
		    }
	    } catch (Exception e) {
	    	logger.error(e.getMessage());
	    } finally {
	    	executor.shutdown();
	    }
	}
	
}
