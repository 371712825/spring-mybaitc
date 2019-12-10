package XiaoTest.practice;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.joda.time.DateTime;

import com.alibaba.fastjson.JSONArray;
import com.google.gson.Gson;

/** 
* @author Lusx 
* @date 2019年5月24日 下午12:21:17 
*/
public class DatePractice {

	public static void main(String[] args) throws Exception, SecurityException {
		Long mb = 1572051509L;
				//1558454400
				//1558627200
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		//String str = sdf.format(mb * 1000);
		String str = sdf.format(1574391600L * 1000);
		String str2 = sdf.format(1569489915653L);
//		System.out.println(str + ">>> " + str2);
		String sdate = "2019-11-24";
		String edate = "2019-11-25";
//		System.out.println(String.valueOf(new DateTime(1564761600L * 1000l).toString("yyyyMMdd")));
		
		Long sdatelong = new DateTime(sdate).getMillis()/1000L;
		Long edatelong = new DateTime(edate).getMillis()/1000L;
		
		
		/*System.out.println(new DateTime().withMinuteOfHour(0));
		System.out.println(new DateTime().withMillisOfSecond(0).getMillis());
		System.out.println(new DateTime().withMillisOfDay(0));*/
		System.out.println(new DateTime("2019-12-01").getMillis()/1000L);
		System.out.println(sdatelong);
		System.out.println(edatelong);
		System.out.println(new DateTime("2019-11-01").getMillis()/1000L);
//		System.out.println(sdf.format(new DateTime("2020-02-02").minusDays(30).getMillis()));
		
//		System.out.println(sdf.format(mb*1000));
		
		/*List<Date> array = getDateByLong(new Date(sdatelong * 1000L),new Date((sdatelong+1) * 1000L));
		System.out.println(sdf.format(array.get(0)));
		System.out.println(sdf.format(array.get(1)));
		System.out.println(JSONArray.parse("[\"2019-06-01T00:00:00.000+08:00/" + new DateTime().toString() + "\"]"));*/
		
//		DateTime end = new DateTime("2019-08-01").withMillisOfDay(0);
//		for (int i=10;i>0;i--) {
//			System.out.println(sdf.format(end.minusDays(i).getMillis()));
//			System.out.println(sdf.format(end.minusDays(i-1).getMillis()));
//		}
		
//		System.out.println(sdf.format(new DateTime("2019-08-08").withMillisOfDay(0).minusDays(0).getMillis()));
//		System.out.println(sdf.format(new DateTime().withMillisOfDay(0).minusDays(1).getMillis()));
	}
	
	/*public static List<Date> getDateByLong(Date stime, Date etime) {
		
		List<Date> lDate = new ArrayList<Date>();
        lDate.add(stime);
        Calendar calBegin = Calendar.getInstance();
        
        calBegin.setTime(stime);
        Calendar calEnd = Calendar.getInstance();
        
        calEnd.setTime(etime);
        
        while (etime.after(calBegin.getTime()))  {
        	
            calBegin.add(Calendar.DAY_OF_MONTH, 1);
            lDate.add(calBegin.getTime());
        }
        return lDate;
	}*/
}
