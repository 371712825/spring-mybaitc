package XiaoTest.practice;

import java.text.SimpleDateFormat;

import org.joda.time.DateTime;

import com.alibaba.fastjson.JSONObject;

public class XiaoPractice {
	
	
	public static void main(String[] args) {
		
		
		Long mb = 1557154720L;
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
		String str = sdf.format(mb*1000);
		
		//System.out.println(new DateTime().withMillisOfDay(0));
		
		System.out.println(str);
		
		JSONObject json = null;
		
		for (int i=0;i<10;i++) {
			json = new JSONObject();
			json.put("kk", i);
			//System.out.println(json.get("kk"));
		}
		
		
		
		
	}
	
}
