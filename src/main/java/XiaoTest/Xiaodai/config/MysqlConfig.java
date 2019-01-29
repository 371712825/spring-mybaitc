package XiaoTest.Xiaodai.config;


import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import XiaoTest.Xiaodai.bo.XConfig;

@Configuration
public class MysqlConfig {
	
	@Value("${datasource.url}")
	private String url;
	
	@Value("${datasource.username}")
	private String username;
	
	@Value("${datasource.password}")
	private String password;

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}
	
	@Bean(name="mysql")
	public XConfig setMysql() {
		XConfig xconfig = new XConfig();
		xconfig.setPassword(password);
		xconfig.setUrl(url);
		xconfig.setUsername(username);
		return xconfig;
	}
	
	@Bean(name="datasource")
	public DataSource getDataSource() {
		DataSourceProperties dataSourceProperties = new DataSourceProperties();
		dataSourceProperties.setUrl(url);
		dataSourceProperties.setUsername(username);
		dataSourceProperties.setPassword(password);
		DataSource dataSource = DataSourceBuilder.create().build();
		return dataSource;
	}
	
}
