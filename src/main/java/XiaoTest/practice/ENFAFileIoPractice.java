package XiaoTest.practice;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.alibaba.fastjson.JSONObject;

import scala.Tuple2;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

public class ENFAFileIoPractice {
	
	
	public static void main(String[] args) throws Exception {
		
		SparkSession spark = SparkSession.builder()
				.appName("Simple Application")
				.master("local")
				.config("spark.sql.crossJoin.enabled", "true")
				.enableHiveSupport()
				.getOrCreate();
		
		Dataset<Row> data = spark.read().json("file:///G:\\\\ENFAT-2019-05-22-2019-05-27.json");
		
		List<JSONObject> infoList = data.toJSON().toJavaRDD().mapToPair(new PairFunction<String, String, Long>() {

			@Override
			public Tuple2<String, Long> call(String t) throws Exception {
				// TODO Auto-generated method stub
				JSONObject json = JSONObject.parseObject(t);
				
				StringBuffer sb = new StringBuffer();
				sb.append(json.getString("uid")).append("-");
				sb.append(json.getString("username")).append("-");
				sb.append(json.getString("bb_status"));
				
				return new Tuple2<String, Long>(sb.toString(), 1L);
			}
		}).reduceByKey(new Function2<Long, Long, Long>() {
			
			@Override
			public Long call(Long v1, Long v2) throws Exception {
				// TODO Auto-generated method stub
				return v1+v2;
			}
		}).flatMap(new FlatMapFunction<Tuple2<String,Long>, JSONObject>() {

			@Override
			public Iterator<JSONObject> call(Tuple2<String, Long> t) throws Exception {
				// TODO Auto-generated method stub
				JSONObject tmp = new JSONObject();
				List<JSONObject> result = new ArrayList<JSONObject>();
				
				String[] tmpStr = t._1.split("-");
				
				tmp.put("uid", tmpStr[0]);
				tmp.put("username", tmpStr[1]);
				tmp.put("bb_status", tmpStr[2]);
				tmp.put("num", t._2);
				
				result.add(tmp);
				
				return result.iterator();
			}
		}).collect();
		
		List<String> list = data.toJSON().toJavaRDD().collect();
		
		HSSFWorkbook hw = new HSSFWorkbook();
		HSSFSheet sheet_detail = hw.createSheet("detail");
		HSSFRow row_detail = sheet_detail.createRow(0);
		
		row_detail.createCell(0).setCellValue("UID");
		row_detail.createCell(1).setCellValue("用户名");
		row_detail.createCell(2).setCellValue("孕期阶段");
		row_detail.createCell(3).setCellValue("访问时间");
		
		int num = 1;
		for (String str : list) {
			JSONObject tmpJson = JSONObject.parseObject(str);
			HSSFRow tmp = sheet_detail.createRow(num);
			
			tmp.createCell(0).setCellValue(tmpJson.getString("uid"));
			tmp.createCell(1).setCellValue(tmpJson.getString("username"));
			tmp.createCell(2).setCellValue(tmpJson.getString("bb_status"));
			tmp.createCell(3).setCellValue(new DateTime(tmpJson.getLong("time")).toString("yyyy-MM-dd"));
			
			num++;
		}
		
		HSSFSheet sheet_info = hw.createSheet("info");
		HSSFRow row_info = sheet_info.createRow(0);
		
		row_info.createCell(0).setCellValue("UID");
		row_info.createCell(1).setCellValue("用户名");
		row_info.createCell(2).setCellValue("孕期阶段");
		row_info.createCell(3).setCellValue("访问次数");
		
		num = 1;
		
		for (JSONObject tmpJson : infoList) {
			HSSFRow tmp = sheet_info.createRow(num);
			
			tmp.createCell(0).setCellValue(tmpJson.getString("uid"));
			tmp.createCell(1).setCellValue(tmpJson.getString("username"));
			tmp.createCell(2).setCellValue(tmpJson.getString("bb_status"));
			tmp.createCell(3).setCellValue(tmpJson.getLong("num"));
			
			num++;
		}

		FileOutputStream out = new FileOutputStream(new File("G:\\ENFAT-2019-05-22.xls"));
		
		hw.write(out);
		
		out.flush();
		out.close();
		
		spark.close();
	}
	
}
