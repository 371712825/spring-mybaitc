package XiaoTest.Xiaodai.controller;

import java.util.Properties;

import javax.annotation.Resource;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;

import XiaoTest.Xiaodai.bo.ModelBo;
import XiaoTest.Xiaodai.bo.Result;
import XiaoTest.Xiaodai.bo.XConfig;
import XiaoTest.Xiaodai.util.HdfsUtils;
import XiaoTest.Xiaodai.util.KafkaConsumer;
import XiaoTest.Xiaodai.util.KafkaProducer;


@RestController
@RequestMapping("basic")
public class BacisController {

	@Resource(name="redis")
	private XConfig redisConfig;
	
	@Resource(name="mysql")
	private XConfig mysqlConfig;
	
	/*@Resource(name="datasource")
	private DataSource dataSource;*/
	
	@Autowired
	private KafkaProducer kafkaProducer = new KafkaProducer();
	
	@RequestMapping("/test")
	public String bacisTest() {
		
		//Jedis jedis = new Jedis(redisConfig.getHost(),redisConfig.getPort());
		/*JedisPool jedisPool1 = new JedisPool(redisConfig.getHost(),redisConfig.getPort());
		Jedis jedis = jedisPool1.getResource();
		
		if (jedis.get("aa")!=null) {
			jedis.set("aa", "22");
		}
		System.out.println(jedis.get("aa"));*/
		
		/*try {
			String str = "select * from user where 1=1 ";
			Connection con = dataSource.getConnection(mysqlConfig.getUsername(), mysqlConfig.getPassword());
			con.createStatement().execute(str);
		} catch (SQLException e) {
			e.printStackTrace();
			
		}
		JdbcTemplateAutoConfiguration jdbcTemplate = new JdbcTemplateAutoConfiguration(dataSource);*/
		
		 /*JdbcTemplate jdbcTemplate = new JdbcTemplate(myDataSource);
	     List<?> resultList = jdbcTemplate.queryForList("select * from menu");*/
		
		return "ok";
	}
	
	@RequestMapping("/kafkaSend")
	public String bacisKafkaSend() {
		
		JSONArray array = new JSONArray();
		ModelBo mb = new ModelBo();
		
		for (int i=0;i<100;i++) {
			mb.setUid(i);
			mb.setName(i+"");
			mb.setOrgId(i);
			
			array.add(mb);
		}
		kafkaProducer.SendMsg(array);
		
		return "work";
	}
	
	@RequestMapping("/kafkaRecevice")
	public String bacisKafkaRecevice() {
		Properties property = new Properties();
		
		//ConsumerConnector cc = Consumer.createJavaConsumerConnector(new ConsumerConfig(property));
		
		Thread t = new Thread(new KafkaConsumer());
		t.start();
		
		return "recevice";
	}
	
	@RequestMapping("/hadoop/setFile")
	public String hadoopSetFile(String filePath) {
		
		try {
			FSDataOutputStream output = HdfsUtils.getInstance().create(new Path("/data/xiaolu/"+filePath), true);
			byte[] tempBytes = null;
			
	    	tempBytes = filePath.getBytes("UTF-8");
			output.write(tempBytes, 0, tempBytes.length);
			
			output.flush();
		    output.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return JSON.toJSONString(new Result());
	}
	
	@RequestMapping("/hadoop/deleteFile")
	public String hadoopDeleteFile(String filePath) {
		
		try {
			HdfsUtils.getInstance().delete(new Path("/data/xiaolu/"+filePath), true);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return JSON.toJSONString(new Result());
	}
	
	public static void main(String[] args) {
		
	}
}
