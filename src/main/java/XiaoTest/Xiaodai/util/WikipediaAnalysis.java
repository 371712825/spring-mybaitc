package XiaoTest.Xiaodai.util;

import org.apache.avro.hadoop.io.AvroSequenceFile.Writer;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

import XiaoTest.Xiaodai.bo.HJsonBo;


public class WikipediaAnalysis {

	private static Logger logger = LoggerFactory.getLogger(WikipediaAnalysis.class);
	
	public static void main(String[] args) throws Exception {

		/*StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		
		DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());
		
		KeyedStream<WikipediaEditEvent, String> keyedEdits = edits
			    .keyBy(new KeySelector<WikipediaEditEvent, String>() {
			        @Override
			        public String getKey(WikipediaEditEvent event) {
			            return event.getUser();
			        }
			    });
		
		DataStream<Tuple2<String, Long>> result = keyedEdits
			    .timeWindow(Time.seconds(5))
			    .fold(new Tuple2<>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
			        @Override
			        public Tuple2<String, Long> fold(Tuple2<String, Long> acc, WikipediaEditEvent event) {
			            acc.f0 = event.getUser();
			            acc.f1 += event.getByteDiff();
			            return acc;
			        }
			    });
		
		//result.print();
		//see.execute();
		
		result
	    .map(new MapFunction<Tuple2<String,Long>, String>() {
	        @Override
	        public String map(Tuple2<String, Long> tuple) {
	            return tuple.toString();
	        }
	    })
	    .addSink(new FlinkKafkaProducer011<>("localhost:9092", "wiki-result", new SimpleStringSchema()));*/
		
		//FSDataOutputStream dfos = HdfsUtils.getInstance().create(new Path("/data/xiaolu/"), true);
		
		try {
			
			StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
			see.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
			see.setParallelism(1);
			see.getConfig().registerKryoType(HJsonBo.class);;
			
			//see.getConfig().registerPojoType(HJsonBo.class);
			
			//see.registerType(HJsonBo.class);
			
			DataStream<String> ds = see.readTextFile(HdfsUtils.HOST + "/data/xiaolu/hadoop/dmp-full.json");
			
			
			ds.filter(new FilterFunction<String>() {
				
				/**
				 * 
				 */
				private static final long serialVersionUID = -4794828835306322033L;

				@Override
				public boolean filter(String value) throws Exception {
					// TODO Auto-generated method stub
					JSONObject data = JSONObject.parseObject(value);
					
					if (data.getString("remote_addr").equals("223.104.23.158")) {
						return true;
					}
					return false;
				}
			}).writeAsText(HdfsUtils.HOST + "/data/xiaolu/hadoop/dmp-full2.json");
			
			//ds.print();
			//see.execute();
			
			//DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());
			
			/*SparkSession spark = SparkSession.builder()
					.appName("Simple Application")
					.master("local")
					.config("spark.sql.crossJoin.enabled", "true")
					.enableHiveSupport()
					.getOrCreate();
			
			Dataset<Row> data = spark.read().format("com.databricks.spark.avro")
					.load("file:///G:\\part.task_Kafka-HDFS-BODY_1554048600076_0_1.avro").limit(1000);
			//.textFile("G:\\part.task_Kafka-HDFS-BODY_1554048600076_0_1.avro");
			
			logger.info("---------------------------start!!!");
			
		    JavaRDD<JSONObject> results=data.toJSON()
		    		.toJavaRDD()
		    		.filter(new Function<String, Boolean>() {
						
						@Override
						public Boolean call(String v1) throws Exception {
							// TODO Auto-generated method stub
							JSONObject data = JSONObject.parseObject(v1);
							boolean isParse = false;
							if(StringUtils.isNotBlank(data.getString("request_body")) && !"-".equals(data.getString("request_body"))){
								if(data.getString("request_body").startsWith("data=")){
									// WEB POST请求
									isParse = false;
								} else {
									// APP POST请求
									isParse = true;
								}
							}else {
								isParse = false;
							}
							return isParse;
						}
					})
		    		.flatMap(new FlatMapFunction<String, JSONObject>(){
		    			public Iterator<JSONObject> call(String message) throws Exception {
		    				List<JSONObject> result = new ArrayList<JSONObject>();
		    				JSONObject data = JSONObject.parseObject(message);
		    				JSONObject tmp = new JSONObject();
		    				
		    				tmp.put("uripath", data.getString("uripath"));
		    				tmp.put("timestamp", data.getString("timestamp"));
		    				tmp.put("remote_addr", data.getString("remote_addr"));
		    		    	
		    	    		result.add(tmp);
		    	    		
		    	    		return result.iterator();
		    			}
		    		});
		    
		    logger.info("count-------------"+results.count());
		    
		    results.mapToPair(new PairFunction<JSONObject, String,Long>() {

				@Override
				public Tuple2<String,Long> call(JSONObject t) throws Exception {
					// TODO Auto-generated method stub
					return new Tuple2<String,Long>(t.toJSONString(),1L);
				}
			}).reduceByKey(new Function2<Long, Long, Long>() {
				
				@Override
				public Long call(Long v1, Long v2) throws Exception {
					// TODO Auto-generated method stub
					return v1+v2;
				}
			}).foreach(x -> System.out.println(x+""));*/
			
		    //results.foreach(x -> System.out.println(x+""));
		    
		    
		    /*Iterator<JSONObject> it = results.collect().iterator();
		    FSDataOutputStream fdos = HdfsUtils.getInstance().create(new Path("/data/xiaolu/hadoop/dmp-full.json"), true);
		    final byte[] newLineBytes = "\n".getBytes("UTF-8");
			byte[] tempBytes = null;
		    
			while (it.hasNext()) {
			    tempBytes = it.next().toJSONString().getBytes("UTF-8");
			    fdos.write(tempBytes, 0, tempBytes.length);
			    fdos.write(newLineBytes, 0, newLineBytes.length);
			}
			
			fdos.flush();
			fdos.close();*/
			
			logger.info("--------------------------------finish!!!");
			
		} catch(Exception e) {
			e.printStackTrace();
			logger.info("--------------------------------error!!!");
		}
		
		
    }
}
