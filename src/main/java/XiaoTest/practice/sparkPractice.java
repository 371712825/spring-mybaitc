package XiaoTest.practice;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;


/** 
* @author Lusx 
* @date 2019年6月17日 上午11:16:20 
*/
public class sparkPractice {

	private static Logger logger = LoggerFactory.getLogger(sparkPractice.class);
	
	public final static long PREGNANT_DURATION = 60 * 60 * 24 * 7 * 1000 * 48l;
	
	public static final long WEEK_DURATION = 60 * 60 * 24 * 7 * 1000l;
	
	public static void main(String[] args) throws Exception {
		
		SparkSession spark = SparkSession.builder()
				.appName("Simple Application")
				.master("local")
				.config("spark.sql.crossJoin.enabled", "true")
				.enableHiveSupport()
				.getOrCreate();
		
		Dataset<Row> row = spark.read().json("file:///G:\\1107.json");
		
		row = row.filter("bbbirth != ''");
		
		Iterator<String> its = row.toJSON().toJavaRDD()
				.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {

					@Override
					public Iterator<String> call(Iterator<String> t) throws Exception {
						// TODO Auto-generated method stub
						List<String> result = new ArrayList<String>();
						
						while (t.hasNext()) {
							JSONObject data = JSONObject.parseObject(t.next());
							
							String bbBirthday = data.getString("bbbirth");
							Integer bbType = data.getInteger("user_type");
							Long dateline = data.getLong("dateline");
							
							if (StringUtils.isBlank(bbBirthday)) {
								continue;
							}
							
							DateTime bbBirtDate = new DateTime(bbBirthday);
							
							if (bbType == 2) {
								if (bbBirtDate.getMillis()/1000L <= dateline) {
									Long bbday = ( dateline*1000 - bbBirtDate.getMillis())
											/ 86400000l;
									
									data.put("dddate", new DateTime(dateline * 1000l).toString("yyyy-MM-dd"));
									result.add(data.toJSONString());
									/*if(bbday *(60 * 60 * 24 * 1000l) < -WEEK_DURATION * 40) {
										data.put("dddate", new DateTime(dateline * 1000l).toString("yyyy-MM-dd"));
										result.add(data.toJSONString());
									}*/
								}
							} 
						}
						
						return result.iterator();
					}
					
				}).toLocalIterator();
		
		FileOutputStream fos = new FileOutputStream("G:\\\\mobileid.csv");
        OutputStreamWriter osw = new OutputStreamWriter(fos, "GBK");
        CSVFormat csvFormat = CSVFormat.DEFAULT.withHeader("mobileid","bbbirth","dddate");
        CSVPrinter csvPrinter = new CSVPrinter(osw, csvFormat);
        
		while (its.hasNext()) {
			String tmp = its.next();
			JSONObject tmpJson = JSONObject.parseObject(tmp);
			csvPrinter.printRecord(tmpJson.getString("mobileid"),tmpJson.getString("bbbirth"),tmpJson.getString("dddate"));
		}
		
		csvPrinter.flush();
        csvPrinter.close();
        fos.close();
		
		/*logger.info("init-long:"+row.count());
		row = row.filter("uid != '-1' and position = 'SEARCH_FLOW_ALL'");
		logger.info("filter-long:"+row.count());
		row = row.select("uid","search_keyword","timestamp");
		//row = row.orderBy("timestamp","DESC");
		//row = row.groupBy("uid").max("timestamp").as("maxtimestamp");
		logger.info("group-long:"+row.count());
		Iterator it = row.toJSON().toLocalIterator();
		int i=0;
		while(it.hasNext() && i<10) {
			String str = it.next().toString();
			logger.info(i+">>>"+str);
			i++;
		}*/
		
		//row = row.filter("event = 'impression'");
		
		/*JavaRDD<String> jrs = row.toJSON().toJavaRDD().filter(new Function<String, Boolean>() {
			
			@Override
			public Boolean call(String v1) throws Exception {
				// TODO Auto-generated method stub
				JSONObject json = JSONObject.parseObject(v1);
				boolean flag = false;
				
				if (json.containsKey("event") && StringUtils.isNotBlank(json.getString("event")) && json.getString("event").equals("impression")) {
					flag = true;
				}
				
				return flag;
			}
		});*/
		
		/*Long uv = row.toJSON().toJavaRDD().mapToPair(new PairFunction<String, String, Long>() {

			@Override
			public Tuple2<String, Long> call(String t) throws Exception {
				// TODO Auto-generated method stub
				JSONObject json = JSONObject.parseObject(t);
				
				
				
				return new Tuple2<String,Long>(json.getString("devOuid"),1L);
			}
		}).reduceByKey(new Function2<Long, Long, Long>() {
			
			@Override
			public Long call(Long v1, Long v2) throws Exception {
				// TODO Auto-generated method stub
				return v1+v2;
			}
		}).count();
		
		System.out.println("pv-Long:"+row.count());
		System.out.println("devOuid-Long:"+uv);*/
		
		/*Iterator<String> itjs = row.toJSON().toLocalIterator();
		
		byte[] newLineBytes = "\n".getBytes("UTF-8");
		byte[] tempBytes = null;
		FileOutputStream output = new FileOutputStream("G:\\hdfs-1.json");
		while (itjs.hasNext()) {
			String tmp = itjs.next();
			
			tempBytes = tmp.getBytes("UTF-8");
			output.write(tempBytes, 0, tempBytes.length);
			output.write(newLineBytes, 0, newLineBytes.length);
		}
		output.flush();
		output.close();*/
	}
	
}
