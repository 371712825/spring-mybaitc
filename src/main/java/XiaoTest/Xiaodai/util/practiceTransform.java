package XiaoTest.Xiaodai.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.broadcast.TorrentBroadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.JsonMappingException;

import jodd.util.URLDecoder;
import scala.Tuple2;
import scala.reflect.ClassTag;

/** 
* 从2019-05-22开始有点位数据 
* @author Lusx 
* @date 2019年5月16日 下午4:00:24 
* 
*/
public class practiceTransform {

	private static Logger logger = LoggerFactory.getLogger(practiceTransform.class);

	private static volatile Broadcast<List<String>> broadcastList = null;
	
	public static void main(String[] args) {
		
		try {
			Long stime = 1558454400L;
			Long etime = 1558540800L;
			
			SparkSession spark = SparkSession.builder()
					.appName("Simple Application")
					.master("local")
					.config("spark.sql.crossJoin.enabled", "true")
					.enableHiveSupport()
					.getOrCreate();
			
			//Dataset<Row> data = spark.read().format("com.databricks.spark.avro").load("file:///G:\\part.task_Kafka-HDFS-BODY_1558541400122_0_0.avro");
			Dataset<Row> data = spark.read().json("file:///G:\\\\search-log-*");
			
			String sdate = new DateTime(stime * 1000L).toString("yyyy-MM-dd");
			
			logger.info("导出关键字数据时间为：" + sdate + "开始");
			//Dataset<Row> data = spark.read().json(HdfsUtils.HOST + "/data/so/search-log-*");
			data = data.filter("uid != '-1'");
			//List<String> uidList = 
			JavaRDD<String> j1 = data
				//.filter("keyword = '大脑发育'")
				.toJSON()
				.toJavaRDD()
				.filter(new Function<String, Boolean>() {
					
					@Override
					public Boolean call(String value) throws Exception {
						// TODO Auto-generated method stub
						JSONObject json = JSONObject.parseObject(value);
						boolean flag = false;
						
						//logger.info(json.getString("keyword"));
						
						if (json.containsKey("bb_status") && json.containsKey("keyword") && "大脑发育".equals(json.getString("keyword"))
								) {
							flag = true;
							/*JSONArray array = json.getJSONArray("bb_status");
							
							for (int i=0,len=array.size();i<len;i++) {
								String bb_status = array.get(i).toString();
								if (bb_status.startsWith("育") && !"育3岁以上".equals(bb_status)) {
									flag = true;
									break;
								}
							}*/
						}
						
						return flag;
					}
				});
			logger.info("j1.long:"+j1.count());
			/*JavaPairRDD<String, Long>  j2 = j1.mapToPair(new PairFunction<String, String, Long>() {

					@Override
					public Tuple2<String, Long> call(String t) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<String,Long>(JSONObject.parseObject(t).getString("uid"),1L);
					}
				});
			
			logger.info("j2.long:"+j2.count());
				
			List<String> uidList = j2
				.reduceByKey(new Function2<Long, Long, Long>() {
					
					@Override
					public Long call(Long v1, Long v2) throws Exception {
						// TODO Auto-generated method stub
						return v1+v2;
					}
				})
				.keys()
				.collect();
			
			logger.info("uidList.long:"+uidList.size());*/
			
			/*uidList.forEach(item->{
				System.out.println(item.toString());
			});
			
			ClassTag<List<String>> classTag = scala.reflect.ClassTag$.MODULE$.apply(List.class);
			broadcastList = spark.sparkContext().broadcast(uidList,classTag);
			
			logger.info("broadcastList:"+broadcastList.value().toArray().toString());
			
			List<JSONObject> soResult = data
						.toJSON()
						.toJavaRDD()
						.filter(new Function<String, Boolean>() {
							
							@Override
							public Boolean call(String v1) throws Exception {
								// TODO Auto-generated method stub
								JSONObject tmp = JSONObject.parseObject(v1);
								String uid = tmp.getString("uid");
								boolean flag = false;
								
								for (String str : broadcastList.value()) {
									if (uid.equals(str)) {
										flag = true;
										break;
									}
								}
								
								return flag;
							}
						})
						.flatMap(new FlatMapFunction<String, JSONObject>() {

							@Override
						    public Iterator<JSONObject> call(String message) throws Exception {
						    	
						    	List<JSONObject> result = new ArrayList<JSONObject>();
					    		
						    	JSONObject json = JSONObject.parseObject(message);
						    	JSONObject resultJson = new JSONObject();
						    	
						    	if (json.containsKey("keyword")) {
						    		resultJson.put("keyword", json.getString("keyword"));
						    	}
						    	
						    	result.add(resultJson);
					    		
						    	return result.iterator(); 
						    }
						})
						.mapToPair(new PairFunction<JSONObject, String, Long>() {

							@Override
							public Tuple2<String, Long> call(JSONObject t) throws Exception {
								// TODO Auto-generated method stub
								return new Tuple2<String,Long>(t.getString("keyword"),1L);
							}
						})
						.reduceByKey(new Function2<Long, Long, Long>() {
							
							@Override
							public Long call(Long v1, Long v2) throws Exception {
								// TODO Auto-generated method stub
								return v1+v2;
							}
						})
						.flatMap(new FlatMapFunction<Tuple2<String,Long>, JSONObject>() {

							@Override
							public Iterator<JSONObject> call(Tuple2<String, Long> t) throws Exception {
								// TODO Auto-generated method stub
								JSONObject tmp = new JSONObject();
								List<JSONObject> result = new ArrayList<JSONObject>();
								
								tmp.put("keyword", t._1);
								tmp.put("num", t._2);
								
								result.add(tmp);
								
								return result.iterator();
							}
						})
						.collect();
			logger.info("size:"+soResult);
			soResult.forEach(item->{
				logger.info(item.toJSONString());
			});*/
			
			/*FSDataOutputStream fsos = HdfsUtils.getInstance().create(new Path("/data/bispark/reportFile/SoKeyword-"+sdate+".json"),true);
			
			byte[] newLineBytes = "\n".getBytes("UTF-8");
			byte[] tempBytes = null;
			
			
			for (JSONObject json : soResult) {
				
				tempBytes = json.toJSONString().getBytes("UTF-8");
				fsos.write(tempBytes, 0, tempBytes.length);
				fsos.write(newLineBytes, 0, newLineBytes.length);
			}
			
			fsos.flush();
			fsos.close();*/
		
		} catch (Exception e) {
			e.printStackTrace();
		}
	
	
	}
}
