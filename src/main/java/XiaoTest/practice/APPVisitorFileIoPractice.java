package XiaoTest.practice;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

import scala.Tuple2;


/** 
* @author Lusx 
* @date 2019年5月29日 上午11:45:08 
*/
public class APPVisitorFileIoPractice {

	private static Logger logger = LoggerFactory.getLogger(APPVisitorFileIoPractice.class);
	
	public static void main (String[] args) throws Exception {
		SparkSession spark = SparkSession.builder()
				.appName("Simple Application")
				.master("local")
				.config("spark.sql.crossJoin.enabled", "true")
				.enableHiveSupport()
				.getOrCreate();
		
		Dataset<Row> data = spark.read().json("file:///G:\\\\AppVisitor-*.json");
		
		
		//Map<String, String> resultMap = 
		Iterator<Tuple2<String, String>>  its =	
				data.toJSON().toJavaRDD()
			.filter(new Function<String, Boolean>() {
				
				@Override
				public Boolean call(String v1) throws Exception {
					// TODO Auto-generated method stub
					JSONObject json = JSONObject.parseObject(v1);
					boolean flag = false;
					
					/*if (json.containsKey("itemIds") && json.getString("itemIds") != null) {
						flag = true;
					}
					String itemIds = json.getString("itemIds");
					
					
					
					//itemid --- 1,2,3,4,5,8
					//item_type --- post
					if (itemIds.contains("1") || itemIds.contains("2") || itemIds.contains("3") || itemIds.contains("4")
						|| itemIds.contains("5") || itemIds.contains("post") || itemIds.contains("151") || itemIds.contains("b_25") 
						|| itemIds.contains("56") || itemIds.contains("52") || itemIds.contains("b_3#p_sl#id_0") || itemIds.contains("57") 
						|| itemIds.contains("129") || itemIds.contains("b_14") || itemIds.contains("131") || itemIds.contains("b_15") 
						|| itemIds.contains("HOME_PAGE_KNOWLEDGE") && itemIds.contains("pt_pregnant") || itemIds.contains("HOME_PAGE_BABY")
						|| itemIds.contains("HOME_PAGE_CHANGE") || itemIds.contains("8")) {
						flag = true;
					}*/
					
					if (json.containsKey("itemIds") && json.getString("itemIds") != null) {
						String itemIds = json.getString("itemIds");
						itemIds = itemIds.substring(1,itemIds.length()-1);
						String[] strs = itemIds.split(",");
						List<String> listStr = Arrays.asList(strs);
						if (listStr.contains("1") || listStr.contains("2") || listStr.contains("3") || listStr.contains("4")
								|| listStr.contains("5") || listStr.contains("post") || listStr.contains("151") || listStr.contains("b_25") 
								|| listStr.contains("56") || listStr.contains("52") || listStr.contains("b_3#p_sl#id_0") || listStr.contains("57") 
								|| listStr.contains("129") || listStr.contains("b_14") || listStr.contains("131") || listStr.contains("b_15") 
								|| listStr.contains("HOME_PAGE_KNOWLEDGE") && listStr.contains("pt_pregnant") || listStr.contains("HOME_PAGE_BABY")
								|| listStr.contains("HOME_PAGE_CHANGE") || listStr.contains("8")) {
								flag = true;
							}
					}
					
					return flag;
				}
			})
			.mapToPair(new PairFunction<String, String, String>() {

			@Override
			public Tuple2<String, String> call(String t) throws Exception {
				// TODO Auto-generated method stub
				JSONObject json = JSONObject.parseObject(t);
				
				String key = json.getString("devid");
				String str =json.getString("itemIds");
				
				str = str.substring(1,str.length()-1);
				
				return new Tuple2<String, String>(key, str);
			}
		}).reduceByKey(new Function2<String, String, String>() {
			
			@Override
			public String call(String v1, String v2) throws Exception {
				// TODO Auto-generated method stub
				return v1 + "," + v2;
			}
		})
		.toLocalIterator();
		//.collectAsMap();
		logger.info("********数据解析完成");
		
		Map<String,String> resultMap = new HashMap<String,String>();
		while (its.hasNext()) {
			Tuple2<String, String> t = its.next();
			resultMap.put(t._1, t._2);
		}
		logger.info("**********map生成成功....");
			
		logger.info("*********文件开始生成");
		FileInputStream fis = new FileInputStream(new File("G:\\\\business-sta\\用户核心行为验证-0528-2.xlsx"));
		XSSFWorkbook xw = new XSSFWorkbook(fis);
		Sheet sheet = xw.getSheet("3月-怀孕");
		//Sheet sheet = xw.getSheet("5月-怀孕");
		//Sheet sheet = xw.getSheet("5月-备孕");
		
		for (int i=2,len=sheet.getPhysicalNumberOfRows();i<len;i++) {
			org.apache.poi.ss.usermodel.Row row = sheet.getRow(i);
			Cell cellid = row.getCell(0);
			
			if (resultMap.containsKey(cellid.toString())) {
				String value = resultMap.get(cellid.toString());
				String[] list = value.split(",");
				Set<String> set = new HashSet<>(Arrays.asList(list));
				
				if (set.contains("1")) {	//能不能吃
					row.createCell(1).setCellValue(1);
				}
				if (set.contains("5")) {	//孕期食谱
					row.createCell(2).setCellValue(1);
				}
				if (set.contains("3")) {	//产检表
					row.createCell(3).setCellValue(1);
				}
				if (set.contains("post")) {	//帖子
					row.createCell(4).setCellValue(1);
				}
				if (set.contains("2")) {	//能不能做
					row.createCell(5).setCellValue(1);
				}
				if (set.contains("4")) {	//B超单解释
					row.createCell(6).setCellValue(1);
				}
				if (set.contains("151") || set.contains("b_25") || set.contains("56") || set.contains("52") || set.contains("b_3#p_sl#id_0") 
						|| set.contains("57") || set.contains("129") || set.contains("b_14") || set.contains("131") || set.contains("b_15")) {	//宝典
					row.createCell(7).setCellValue(1);
				}
				if (set.contains("HOME_PAGE_KNOWLEDGE") && set.contains("pt_pregnant")) {
					row.createCell(8).setCellValue(1);
				}
				if (set.contains("HOME_PAGE_BABY") && set.contains("pt_pregnant")) {
					row.createCell(9).setCellValue(1);
				}
				if (set.contains("HOME_PAGE_CHANGE") && set.contains("pt_pregnant")) {
					row.createCell(10).setCellValue(1);
				}
				if (set.contains("8")) {	//生男生女
					row.createCell(11).setCellValue(1);
				}
			}
		}
		
		logger.info("**********解析完成");
		FileOutputStream output = new FileOutputStream("G:\\3月-怀孕.xlsx");
		
		
		
		
		xw.write(output);
		logger.info("**********文件生成");
		output.flush();
		output.close();
		fis.close();
		
		spark.close();
	}
	
}
