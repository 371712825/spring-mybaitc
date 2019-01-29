package XiaoTest.Xiaodai.controller;

import java.sql.Connection;
import java.sql.SQLException;

import javax.annotation.Resource;
import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import XiaoTest.Xiaodai.bo.XConfig;

@RestController
@RequestMapping("basic")
public class BacisController {

	@Resource(name="redis")
	private XConfig redisConfig;
	
	@Resource(name="mysql")
	private XConfig mysqlConfig;
	
	@Resource(name="datasource")
	private DataSource dataSource;
	
	//private DataSource dataSource;
	
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
	
	public static void main(String[] args) {
		
	}
}
