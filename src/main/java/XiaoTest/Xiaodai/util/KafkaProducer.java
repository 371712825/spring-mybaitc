package XiaoTest.Xiaodai.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSONArray;

@Component
public class KafkaProducer {

	@Autowired
	private KafkaTemplate kafkaTemplate;
	
	private final static Logger logger = LoggerFactory.getLogger(KafkaProducer.class);
	
	public void SendMsg(JSONArray array) {
		
		logger.info("发送数据集:{}",array.toJSONString());
		try {
			kafkaTemplate.send("partition-test",array);
			logger.info("发送完毕");
		} catch (Exception e) {
			e.printStackTrace();
			logger.info("发送失败");
		}
	}
	
}
