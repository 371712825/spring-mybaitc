package XiaoTest.Xiaodai.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCUtils {
	private static Logger logger = LoggerFactory.getLogger(JDBCUtils.class);

	static {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			logger.error(e.getMessage(), e);
		}
	}

	public static void execute(String sql) {
		try {
			Connection conn = JDBCUtils.getJdbcConn();
			conn.createStatement();
			Statement st = conn.createStatement();// 准备执行语句
			st.execute(sql);
			st.close();
			conn.close();
		} catch (SQLException e) {
			logger.error(e.getMessage(), e);
		}
	}
	
	public static Map<String,String> executeQuery(String sql) {
		Map<String,String> result = new HashMap<String,String>();
		try {
			Connection conn = JDBCUtils.getJdbcConn();
			conn.createStatement();
			Statement st = conn.createStatement();// 准备执行语句
			ResultSet resultSet = st.executeQuery(sql);
			ResultSetMetaData metaData = resultSet.getMetaData();
			int columnCount = metaData.getColumnCount();			
			while(resultSet.next()) {
				for(int i=1;i<=columnCount;i++) {
					result.put(metaData.getColumnName(i),resultSet.getString(metaData.getColumnName(i)));
				}
			}
			st.close();
			conn.close();
		} catch (SQLException e) {
			logger.error(e.getMessage(), e);
		}
		return result;
	}
	
	public static List<Map<String,Object>> executeQueryList(String sql) {
		List<Map<String,Object>> results = new ArrayList<Map<String,Object>>();
		try {
			Connection conn = JDBCUtils.getJdbcConn();
			conn.createStatement();
			Statement st = conn.createStatement();// 准备执行语句
			ResultSet resultSet = st.executeQuery(sql);
			ResultSetMetaData metaData = resultSet.getMetaData();
			int columnCount = metaData.getColumnCount();			
			while(resultSet.next()) {
				Map<String, Object> result = new HashMap<String,Object>();
				for(int i=1;i<=columnCount;i++) {
					result.put(metaData.getColumnName(i),resultSet.getObject(metaData.getColumnName(i)));
				}
				results.add(result);
			}
			st.close();
			conn.close();
		} catch (SQLException e) {
			logger.error(e.getMessage(), e);
		}
		return results;
	}

	public static Connection getJdbcConn() {
		try {
			return DriverManager.getConnection(JDBCUtils.getJdbcUrl(),
					"bi.username",
					"bi.password");
		} catch (SQLException e) {
			logger.error(e.getMessage(), e);
		}
		return null;
	}

	public static String getJdbcUrl() {
		// example
		// jdbc:mysql://10.0.0.120:3306/bi_report?useUnicode=true&characterEncoding=utf8
		return "jdbc:mysql://" + "host" + ":"
				+ "port" + "/"
				+ "database" + "?useUnicode=true&characterEncoding=utf8";
	}

	public static Properties getJdbcConnUsernamePasswdProperties(String database) {
		Properties connectionProperties = new Properties();
		connectionProperties.setProperty("user", "username");// 设置用户名
		connectionProperties.setProperty("password", "password");// 设置密码
		return connectionProperties;
	}
}
