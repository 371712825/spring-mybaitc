package XiaoTest.practice;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;

import com.alibaba.fastjson.JSONObject;

import XiaoTest.Xiaodai.util.HttpsUtils;

import io.searchbox.core.SearchResult;

/** 
* @author Lusx 
* @date 2019年6月28日 上午10:44:34 
*/
public class esPractice {

	public static void main(String[] args) throws Exception{
		InputStream fr = new FileInputStream(new File("G:\\bxkUids.json"));
		BufferedReader br = new BufferedReader(new InputStreamReader(fr));
		
		Map<String, String> header = new HashMap<String, String>();
		header.put("Content-Type", "application/json;charset=utf-8");
		header.put("Accept", "application/json");
		
		String str = null;
		while (true) {
			str = br.readLine();
			if(str!=null) {
				String result = HttpsUtils.doGet("http://kopf.bjmama.com.cn/persona/latest/"+str, header, null, "UTF-8");
				try {
					JSONObject json = JSONObject.parseObject(result);
				} catch (Exception e) {
					System.out.println(result + "错误uid：" + str);
				}
				
			} else {
				break;
			}
		}
		
		fr.close();
		br.close();
	}
}
