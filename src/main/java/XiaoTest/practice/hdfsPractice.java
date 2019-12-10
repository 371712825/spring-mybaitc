package XiaoTest.practice;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

import XiaoTest.Xiaodai.util.HdfsUtils;

/** 
* @author Lusx 
* @date 2019年5月27日 下午6:16:22 
*/
public class hdfsPractice {

	private static Logger logger = LoggerFactory.getLogger(hdfsPractice.class);

	public static void main(String[] args) throws Exception {
		
			SparkSession spark = SparkSession.builder()
				.appName("Simple Application")
				.master("local")
				.config("spark.sql.crossJoin.enabled", "true")
				.enableHiveSupport()
				.getOrCreate();
			
			//Dataset<Row> row = spark.read().format("com.databricks.spark.avro").load("file:///G:\\2019-04-18-0.avro");
			//Dataset<Row> row = spark.read().json(HdfsUtils.HOST+"/data/append-{3,5,6*}");
			//FSDataOutputStream output = HdfsUtils.getInstance().create(new Path("/data/xiaolu/hadoop/0828-new.json"), true);
			
			
			FileInputStream input = new FileInputStream(new File("G:\\lp.json"));
			BufferedReader br = new BufferedReader(new InputStreamReader(input));
			
			String str;
			FileOutputStream output = new FileOutputStream("G:\\\\lp.xlsx");
			XSSFWorkbook xw = new XSSFWorkbook();
			Sheet sheet = xw.createSheet("Sheet1");
			int i = 1;
			while ((str = br.readLine())!= null) {
				JSONObject json = JSONObject.parseObject(str);
				org.apache.poi.ss.usermodel.Row row = sheet.createRow(i);
				
				row.createCell(0).setCellValue(json.getString("date_mon"));
				row.createCell(1).setCellValue(json.getString("goods_name"));
				row.createCell(2).setCellValue(json.getString("nummm"));
				
				i++;
			}
			
			System.out.println("success>>>>>>>>>>>>>>>>>>>>>>>>>>>");
			
			xw.write(output);
			output.flush();
			output.close();
			input.close();
			
			//Dataset<Row> row = spark.read().json(HdfsUtils.HOST + "/data/xiaolu/hadoop/0501.json");
			
			//DruidIOUtils.sendTaskToDruid("/data/xiaolu/hadoop/0601.json", "xiaolu-click", "P1D", ClickBo.class);
			
	}
	
	/*private class ClickBo{
		
		@TimeStamp
		private Long timestamp;
		
		private String item_type;
		
		@HyperUnique
		private String devOuid;
		
		private String pv;
		
		private String itemid;
		
		private String uid;

		public Long getTimestamp() {
			return timestamp;
		}

		public String getItem_type() {
			return item_type;
		}

		public String getDevOuid() {
			return devOuid;
		}

		public String getPv() {
			return pv;
		}

		public String getItemid() {
			return itemid;
		}

		public String getUid() {
			return uid;
		}

		public void setTimestamp(Long timestamp) {
			this.timestamp = timestamp;
		}

		public void setItem_type(String item_type) {
			this.item_type = item_type;
		}

		public void setDevOuid(String devOuid) {
			this.devOuid = devOuid;
		}

		public void setPv(String pv) {
			this.pv = pv;
		}

		public void setItemid(String itemid) {
			this.itemid = itemid;
		}

		public void setUid(String uid) {
			this.uid = uid;
		}
		
		
	}*/
	
}
