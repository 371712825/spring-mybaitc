package XiaoTest.practice;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import XiaoTest.Xiaodai.bo.ModelBo;
import XiaoTest.Xiaodai.util.JsonUtils;

/** 
* @author Lusx 
* @date 2019年5月21日 上午11:37:24 
*/
public class MsgPractice {

	private static final Map<String,String> map;
	
	static {
		map = new HashMap<String,String>();
		map.put("1", "1");
		map.put("2", "2");
		map.put("3", "3");
	}
	
	public static void main(String[] args) throws Exception {
		String str = "\\x84\\xA5event\\xA5click\\xA4user\\x8D\\xA4wifi\\xA11\\xA6os_ver\\xA19\\xA2os\\xA7android\\xA8latitude\\xA935.067674\\xACuser_bbbirth\\xAA2020-07-21\\xA8user_sex\\xA11\\xA3uid\\xA823796800\\xA4adid\\xDA\\x00\\x22b5b2d61c98de6757ce3b96e54d9b6678f1\\xA9user_type\\xA12\\xA6vendor\\xA5HONOR\\xA4imei\\xAF865893047395548\\xA5model\\xA9HRY-AL00T\\xA9longitude\\xAA117.549282\\xA7content\\x86\\xA3app\\xA2pt\\xAEsearch_keyword\\xA0\\xA7app_ver\\xA58.3.1\\xA6action\\xA0\\xACcontext_type\\xA0\\xA9contextid\\xA0\\xAAproperties\\x91\\x86\\xA6itemid\\xA3192\\xA9item_type\\xA4tool\\xACclose_reason\\xA0\\xA8position\\xB3TOOL_FOUND_TOOLLIST\\xA9sessionid\\xA8270760ac\\xA4time\\xAD1574940603787";
		
		JSONObject json = MsgPackUtils.getDeserializedJSONObject(str);
		//System.out.println(json.toJSONString());
		
		String mamaspm = "103.3.1.5.spm_keyword.mamaquan.mamaquan_3719049.1.uyNu1pqP.cGpfdGl0bGU=";
		String so_param[] = mamaspm.split("\\.");
		
//		System.out.println(Arrays.asList(so_param));
//		System.out.println(so_param[6]);
		/*String DMP_HOTTOPIC = "[{\"version\": \"v1\", \"timestamp\": \"2019-08-10T00:00:00.000+08:00\", \"event\": {\"devOuid_countd_1\": 17780.353777436427, \"devOuid_countd_7\": 1077.7463758205165 } }, {\"version\": \"v1\", \"timestamp\": \"2019-08-11T00:00:00.000+08:00\", \"event\": {\"devOuid_countd_1\": 26880.116441687536, \"devOuid_countd_7\": 1453.8508153668322 } }, {\"version\": \"v1\", \"timestamp\": \"2019-08-12T00:00:00.000+08:00\", \"event\": {\"devOuid_countd_1\": 22821.09399918852, \"devOuid_countd_7\": 1181.9044143147064 } } ]";
		
		List<String> days = new ArrayList<String>();
		String sdate = "2019-08-29";
		String edate = "2019-09-04";
    	SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
    	Long stime = new DateTime(sdate).getMillis()/1000L;
    	Long etime = new DateTime(edate).getMillis()/1000L;
    	days.add(edate);
    	
    	while(stime<etime) {
    		etime = new DateTime(edate).withMillisOfDay(0).minusDays(1).getMillis()/1000L;
    		edate = sdf.format(etime*1000);
    		days.add(edate);
    	}
		System.out.println(StringUtils.join(days,","));*/
		
		//this.getClass().getResource("/");
		/*String st = "5ZO65Lmz5YaF6KGjfDQ2NTMwfDF8MjB8MzN8MGFhOTJlN2E=";
		String so_param[] = new String(Base64.decodeBase64(st))
				.split("\\|");
		
		System.out.println(Arrays.asList(so_param));*/
		
		List<JSONObject> listjson = new ArrayList<JSONObject>();
		JSONObject json1 = new JSONObject();
		json1.put("video", "0");
		json1.put("id","1");
		
		
		JSONObject json2 = JSONObject.parseObject(json1.toJSONString());
		json2.put("video", "1");
		json2.put("id", "2");
		
		listjson.add(json2);
		listjson.add(json1);

		System.out.println(Arrays.asList(listjson));
		
	}
	
}
