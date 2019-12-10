package XiaoTest.practice;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import com.alibaba.fastjson.JSONObject;

/** 
* @author Lusx 
* @date 2019年10月30日 下午4:46:55 
*/
public class CsvPractice {

	public static void main(String[] args) throws Exception{
		
		FileInputStream fisinit = new FileInputStream(new File("G:\\\\xsx_video_time"));
		BufferedReader br = new BufferedReader(new InputStreamReader(fisinit));
		
		FileOutputStream fos = new FileOutputStream("G:\\\\xsx_video_time.csv");
        OutputStreamWriter osw = new OutputStreamWriter(fos, "GBK");

        CSVFormat csvFormat = CSVFormat.DEFAULT.withHeader("日期","浏览次数","人数");
        CSVPrinter csvPrinter = new CSVPrinter(osw, csvFormat);//        csvPrinter = CSVFormat.DEFAULT.withHeader("姓名", "年龄", "家乡").print(osw);

        String str;
        while((str = br.readLine()) != null) {
        	JSONObject json = JSONObject.parseObject(str);
        	csvPrinter.printRecord(json.getString("md"),json.getString("videotimes"),json.getString("ccc"));
		}

        csvPrinter.flush();
        csvPrinter.close();
        fisinit.close();
        fos.close();
	}
}
