package XiaoTest.Xiaodai.util;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import XiaoTest.Xiaodai.druid.annotation.DataSketches;
import XiaoTest.Xiaodai.druid.annotation.DoubleLast;
import XiaoTest.Xiaodai.druid.annotation.DoubleMax;
import XiaoTest.Xiaodai.druid.annotation.DoubleSum;
import XiaoTest.Xiaodai.druid.annotation.HyperUnique;
import XiaoTest.Xiaodai.druid.annotation.Ignore;
import XiaoTest.Xiaodai.druid.annotation.LongLast;
import XiaoTest.Xiaodai.druid.annotation.LongMax;
import XiaoTest.Xiaodai.druid.annotation.LongSum;
import XiaoTest.Xiaodai.druid.annotation.TimeStamp;

public class DruidIOUtils {
	private static Logger logger = LoggerFactory.getLogger(DruidIOUtils.class);
	
	private final static String submitTaskUrl = "http://10.0.0.128:8090/druid/indexer/v1/task";
	private final static String taskStatusUrl = "http://druid-overload.corp.mama.cn/druid/indexer/v1/task/{$TASK_ID}/status";
	
	private static final String sampleTask = "{\"type\":\"index_hadoop\",\"spec\":{\"ioConfig\":{\"type\":\"hadoop\",\"inputSpec\":{\"type\":\"static\",\"paths\":\"hdfs://hadoop2:8020/data/adData/{$DATE}/visit.json\"}},\"dataSchema\":{\"dataSource\":\"growing-visit\",\"granularitySpec\":{\"type\":\"uniform\",\"segmentGranularity\":\"hour\",\"queryGranularity\":\"hour\"},\"parser\":{\"type\":\"hadoopyString\",\"parseSpec\":{\"format\":\"json\",\"dimensionsSpec\":{\"dimensions\":[\"userid\",\"sessionid\",\"sendtime\",\"eventtime\",\"eventtype\",\"ip\",\"countryname\",\"region\",\"city\",\"domain\",\"path\",\"refer\",\"useragent\",\"appversion\",\"model\",\"manufacturer\",\"channel\",\"language\",\"osversion\",\"resolution\",\"platform\",\"visit_id\",\"query\"]},\"timestampSpec\":{\"format\":\"auto\",\"column\":\"timestamp\"}}},\"metricsSpec\":[{\"name\":\"count\",\"type\":\"count\"}]},\"tuningConfig\":{\"type\":\"hadoop\",\"partitionsSpec\":{\"type\":\"hashed\",\"targetPartitionSize\":5000000},\"jobProperties\":{}}}}";
	
	/**
	 * 提交任务，返回taskId
	 * @param taskJson
	 * @return
	 * @throws Exception
	 */
	public static String submitTask(String taskJson) throws Exception{
		Map<String, String> header = new HashMap<String, String>();
		header.put("Content-Type", "application/json;charset=utf-8");
		header.put("Accept", "application/json");
		String uuid = UUID.randomUUID().toString().replaceAll("-", "");
		logger.info("往 druid 提交task {}:{}", uuid, taskJson);
		String result = HttpsUtils.doPost(submitTaskUrl, header, taskJson, "UTF8");
		logger.info("返回 druid 提交 task-{} 结果:{}", uuid, result);
		return JSONObject.parseObject(result).getString("task");
	}
	
	/**
	 * task有三种状态: RUNNING SUCCESS FAILED，本方法中，task状态为SUCCESS FAILED都会返回true。
	 * @param taskId
	 * @return
	 */
	public static int isTaskDone(String taskId){
		Map<String, String> header = new HashMap<String, String>();
		header.put("Content-Type", "application/json;charset=utf-8");
		header.put("Accept", "application/json");
		//example:{"task":"index_hadoop_growing-visit_2017-02-17T12:05:33.357+08:00","status":{"id":"index_hadoop_growing-visit_2017-02-17T12:05:33.357+08:00","status":"SUCCESS","duration":148526}}
		JSONObject result = JSONObject.parseObject(HttpsUtils.doGet(taskStatusUrl.replace("{$TASK_ID}", taskId), header, null, "UTF8"));
		/*
		if("SUCCESS".equals(result.getJSONObject("status").getString("status")) || "FAILED".equals(result.getJSONObject("status").getString("status"))){
			return true;
		}
		return false;
		*/
		if("SUCCESS".equals(result.getJSONObject("status").getString("status")))
			return 1;
		if("FAILED".equals(result.getJSONObject("status").getString("status")))
			return -1;
		return 0;
	}
	
	public static <T> String sendTaskToDruid(String savePath, String dataSource, String granularity, Class<T> clazz) throws Exception {
		String task = sampleTask;
    	JSONObject taskJson = JSONObject.parseObject(task);
    	
    	//设置数据读取路径
    	taskJson.getJSONObject("spec").getJSONObject("ioConfig").getJSONObject("inputSpec").put("paths", HdfsUtils.HOST + savePath);
    	
    	//设置存放的dataSource
    	taskJson.getJSONObject("spec").getJSONObject("dataSchema").put("dataSource", dataSource);
    	
    	JSONArray dimensions = new JSONArray();
    	
    	Field[] fields = clazz.getDeclaredFields();
    	for(Field field : fields) {
    		if(field.isAnnotationPresent(Ignore.class)) {
    			continue;
    		} else if(field.isAnnotationPresent(TimeStamp.class)) {
    			taskJson.getJSONObject("spec").getJSONObject("dataSchema").getJSONObject("parser").getJSONObject("parseSpec").getJSONObject("timestampSpec").put("column", field.getName());
    		} else if(field.isAnnotationPresent(LongSum.class)) {
    			JSONObject obj = new JSONObject();
    			obj.put("type", "longSum");
    			obj.put("name", field.getName());
    			obj.put("fieldName", field.getName());
    			taskJson.getJSONObject("spec").getJSONObject("dataSchema").getJSONArray("metricsSpec").add(obj);
    		} else if(field.isAnnotationPresent(DoubleSum.class)) {
    			JSONObject obj = new JSONObject();
    			obj.put("type", "doubleSum");
    			obj.put("name", field.getName());
    			obj.put("fieldName", field.getName());
    			taskJson.getJSONObject("spec").getJSONObject("dataSchema").getJSONArray("metricsSpec").add(obj);
    		} else if(field.isAnnotationPresent(LongMax.class)) {
    			JSONObject obj = new JSONObject();
    			obj.put("type", "longMax");
    			obj.put("name", field.getName());
    			obj.put("fieldName", field.getName());
    			taskJson.getJSONObject("spec").getJSONObject("dataSchema").getJSONArray("metricsSpec").add(obj);
    		} else if(field.isAnnotationPresent(DoubleMax.class)) {
    			JSONObject obj = new JSONObject();
    			obj.put("type", "doubleMax");
    			obj.put("name", field.getName());
    			obj.put("fieldName", field.getName());
    			taskJson.getJSONObject("spec").getJSONObject("dataSchema").getJSONArray("metricsSpec").add(obj);
    		} else if(field.isAnnotationPresent(HyperUnique.class)) {
    			JSONObject obj = new JSONObject();
    			obj.put("type", "hyperUnique");
    			obj.put("name", field.getName());
    			obj.put("fieldName", field.getName());
    			taskJson.getJSONObject("spec").getJSONObject("dataSchema").getJSONArray("metricsSpec").add(obj);
    		} else if(field.isAnnotationPresent(DataSketches.class)) {
    			JSONObject obj = new JSONObject();
    			obj.put("type", "thetaSketch");
    			obj.put("name", field.getName());
    			obj.put("fieldName", field.getName());
    			taskJson.getJSONObject("spec").getJSONObject("dataSchema").getJSONArray("metricsSpec").add(obj);
    		} else if(field.isAnnotationPresent(DoubleLast.class)) {
    			JSONObject obj = new JSONObject();
    			obj.put("type", "doubleLast");
    			obj.put("name", field.getName());
    			obj.put("fieldName", field.getName());
    			taskJson.getJSONObject("spec").getJSONObject("dataSchema").getJSONArray("metricsSpec").add(obj);
    		} else if(field.isAnnotationPresent(LongLast.class)) {
    			JSONObject obj = new JSONObject();
    			obj.put("type", "longLast");
    			obj.put("name", field.getName());
    			obj.put("fieldName", field.getName());
    			taskJson.getJSONObject("spec").getJSONObject("dataSchema").getJSONArray("metricsSpec").add(obj);
    		} else {
    			dimensions.add(field.getName());
    		}
    	}
    	
    	//设置维度列
    	taskJson.getJSONObject("spec").getJSONObject("dataSchema").getJSONObject("parser").getJSONObject("parseSpec").getJSONObject("dimensionsSpec").put("dimensions", dimensions);
    	
    	//设置存储及查询时间粒度
    	taskJson.getJSONObject("spec").getJSONObject("dataSchema").getJSONObject("granularitySpec").put("segmentGranularity", JSONObject.parse("{\"period\": \"" + granularity + "\", \"timeZone\": \"Asia/Shanghai\", \"type\": \"period\"}"));
    	taskJson.getJSONObject("spec").getJSONObject("dataSchema").getJSONObject("granularitySpec").put("queryGranularity", JSONObject.parse("{\"period\": \"" + granularity + "\", \"timeZone\": \"Asia/Shanghai\", \"type\": \"period\"}"));
    	taskJson.getJSONObject("spec").getJSONObject("dataSchema").getJSONObject("parser").getJSONObject("parseSpec").getJSONObject("timestampSpec").put("format", "millis");
    	
    	//兼容 0.10.0
    	JSONArray intervals = new JSONArray();
    	//良品订单第一单的时间是，2016-05-03，从改天开始获取 interval
    	DateTime startIntervals = DateTime.parse("20160503", DateTimeFormat.forPattern("yyyyMMdd"));
    	DateTime now  = new DateTime();
    	while(startIntervals.isBefore(now)) {
    		intervals.add(startIntervals.toString() + "/" + startIntervals.plusMonths(1).toString());
    		startIntervals = startIntervals.plusMonths(1);
    	}
    		
    	taskJson.getJSONObject("spec").getJSONObject("dataSchema").getJSONObject("granularitySpec").put("intervals", intervals);
    	
    	task = taskJson.toJSONString();
    	
    	return DruidIOUtils.submitTask(task);
	}
	
	public <T> String getTask(String savePath, String dataSource, String granularity, Class<T> clazz) throws Exception{
		
		String task = sampleTask;
    	JSONObject taskJson = JSONObject.parseObject(task);
    	
    	//设置数据读取路径
    	taskJson.getJSONObject("spec").getJSONObject("ioConfig").getJSONObject("inputSpec").put("paths", HdfsUtils.HOST + savePath);
    	
    	//设置存放的dataSource
    	taskJson.getJSONObject("spec").getJSONObject("dataSchema").put("dataSource", dataSource);
    	
    	JSONArray dimensions = new JSONArray();
    	
    	Field[] fields = clazz.getDeclaredFields();
    	for(Field field : fields) {
    		if(field.isAnnotationPresent(Ignore.class)) {
    			continue;
    		} else if(field.isAnnotationPresent(TimeStamp.class)) {
    			taskJson.getJSONObject("spec").getJSONObject("dataSchema").getJSONObject("parser").getJSONObject("parseSpec").getJSONObject("timestampSpec").put("column", field.getName());
    		} else if(field.isAnnotationPresent(LongSum.class)) {
    			JSONObject obj = new JSONObject();
    			obj.put("type", "longSum");
    			obj.put("name", field.getName());
    			obj.put("fieldName", field.getName());
    			taskJson.getJSONObject("spec").getJSONObject("dataSchema").getJSONArray("metricsSpec").add(obj);
    		} else if(field.isAnnotationPresent(DoubleSum.class)) {
    			JSONObject obj = new JSONObject();
    			obj.put("type", "doubleSum");
    			obj.put("name", field.getName());
    			obj.put("fieldName", field.getName());
    			taskJson.getJSONObject("spec").getJSONObject("dataSchema").getJSONArray("metricsSpec").add(obj);
    		} else if(field.isAnnotationPresent(LongMax.class)) {
    			JSONObject obj = new JSONObject();
    			obj.put("type", "longMax");
    			obj.put("name", field.getName());
    			obj.put("fieldName", field.getName());
    			taskJson.getJSONObject("spec").getJSONObject("dataSchema").getJSONArray("metricsSpec").add(obj);
    		} else if(field.isAnnotationPresent(DoubleMax.class)) {
    			JSONObject obj = new JSONObject();
    			obj.put("type", "doubleMax");
    			obj.put("name", field.getName());
    			obj.put("fieldName", field.getName());
    			taskJson.getJSONObject("spec").getJSONObject("dataSchema").getJSONArray("metricsSpec").add(obj);
    		} else if(field.isAnnotationPresent(HyperUnique.class)) {
    			JSONObject obj = new JSONObject();
    			obj.put("type", "hyperUnique");
    			obj.put("name", field.getName());
    			obj.put("fieldName", field.getName());
    			taskJson.getJSONObject("spec").getJSONObject("dataSchema").getJSONArray("metricsSpec").add(obj);
    		} else if(field.isAnnotationPresent(DataSketches.class)) {
    			JSONObject obj = new JSONObject();
    			obj.put("type", "thetaSketch");
    			obj.put("name", field.getName());
    			obj.put("fieldName", field.getName());
    			taskJson.getJSONObject("spec").getJSONObject("dataSchema").getJSONArray("metricsSpec").add(obj);
    		} else if(field.isAnnotationPresent(DoubleLast.class)) {
    			JSONObject obj = new JSONObject();
    			obj.put("type", "doubleLast");
    			obj.put("name", field.getName());
    			obj.put("fieldName", field.getName());
    			taskJson.getJSONObject("spec").getJSONObject("dataSchema").getJSONArray("metricsSpec").add(obj);
    		} else if(field.isAnnotationPresent(LongLast.class)) {
    			JSONObject obj = new JSONObject();
    			obj.put("type", "longLast");
    			obj.put("name", field.getName());
    			obj.put("fieldName", field.getName());
    			taskJson.getJSONObject("spec").getJSONObject("dataSchema").getJSONArray("metricsSpec").add(obj);
    		} else {
    			dimensions.add(field.getName());
    		}
    	}
    	
    	//设置维度列
    	taskJson.getJSONObject("spec").getJSONObject("dataSchema").getJSONObject("parser").getJSONObject("parseSpec").getJSONObject("dimensionsSpec").put("dimensions", dimensions);
    	
    	//设置存储及查询时间粒度
    	taskJson.getJSONObject("spec").getJSONObject("dataSchema").getJSONObject("granularitySpec").put("segmentGranularity", JSONObject.parse("{\"period\": \"" + granularity + "\", \"timeZone\": \"Asia/Shanghai\", \"type\": \"period\"}"));
    	taskJson.getJSONObject("spec").getJSONObject("dataSchema").getJSONObject("granularitySpec").put("queryGranularity", JSONObject.parse("{\"period\": \"" + granularity + "\", \"timeZone\": \"Asia/Shanghai\", \"type\": \"period\"}"));
    	taskJson.getJSONObject("spec").getJSONObject("dataSchema").getJSONObject("parser").getJSONObject("parseSpec").getJSONObject("timestampSpec").put("format", "millis");
    	
    	//兼容 0.10.0
    	JSONArray intervals = new JSONArray();
    	//良品订单第一单的时间是，2016-05-03，从改天开始获取 interval
    	DateTime startIntervals = DateTime.parse("20160503", DateTimeFormat.forPattern("yyyyMMdd"));
    	DateTime now  = new DateTime();
    	while(startIntervals.isBefore(now)) {
    		intervals.add(startIntervals.toString() + "/" + startIntervals.plusMonths(1).toString());
    		startIntervals = startIntervals.plusMonths(1);
    	}
    		
    	taskJson.getJSONObject("spec").getJSONObject("dataSchema").getJSONObject("granularitySpec").put("intervals", intervals);
    	
    	task = taskJson.toJSONString();
    	
    	return task;
	}
}
