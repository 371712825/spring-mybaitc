package XiaoTest.practice;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/** 
* @author Lusx 
* @date 2019年9月9日 下午3:46:38 
*/
public class JSONThread implements Runnable {

	private String str;
	
	private boolean type;
	
	JSONThread(String str, boolean type){
		this.str = str;
		this.type = type;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		Long tt = 0L;
		Long tj = 0L;
		String name;
		if (type) {
			Long t1 = System.currentTimeMillis();
			JSONObject json = JSONObject.parseObject(str);
			Long t2 = System.currentTimeMillis();
			tt = tt+(t2-t1);
			
			Long t3 = System.currentTimeMillis();
			json.toJSONString();
			Long t4 = System.currentTimeMillis();
			tj = tj+(t4-t3);
			
			name = "json-";
		} else {
			Long t1 = System.currentTimeMillis();
			JsonElement json = new Gson().fromJson(str, JsonObject.class);
			Long t2 = System.currentTimeMillis();
			tt = tt+(t2-t1);
			
			Long t3 = System.currentTimeMillis();
			new Gson().toJson(json);
			Long t4 = System.currentTimeMillis();
			tj = tj+(t4-t3);
			
			name = "gson-";
		}
		
		System.out.println(name + tt + "-" + tj);
	}

}
