package XiaoTest.practice;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import XiaoTest.Xiaodai.bo.ModelBo;

public class FileIoPractice {
	
	
	public static void main(String[] args) throws Exception {
		
		
		/*Long mb = 1542012161L;
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
		String str = sdf.format(mb*1000);
		
		String date = "2019-01-01";
		Long datelong = new DateTime(date).getMillis()/1000L;
		
		System.out.println(datelong);
		System.out.println(str);*/
		
		FileOutputStream out = new FileOutputStream(new File("G:\\zong.xls"));
		
		FileInputStream fs1 = new FileInputStream("G:\\pc.xls");
		FileInputStream fs2 = new FileInputStream("G:\\app.xls");
		
		HSSFWorkbook hb1 = new HSSFWorkbook(fs1);
		HSSFWorkbook hb2 = new HSSFWorkbook(fs2);
		
		HSSFSheet sheet1 = hb1.getSheetAt(0);
		HSSFSheet sheet2 = hb2.getSheetAt(0);
		
		Map<String,ModelBo> map = new HashMap<>();
		
		for (int i=2,len=sheet1.getLastRowNum();i<len;i++) {
			 HSSFRow row = sheet1.getRow(i);
			 
			 HSSFCell cell0 = row.getCell(0);
			 String key = cell0.getStringCellValue();
			 
			 HSSFCell cell1 = row.getCell(1);
			 Double value = cell1.getNumericCellValue();
			
			 ModelBo mb = new ModelBo();
			 mb.setAge(value);
			 
			 map.put(key, mb);
		}
		
		for (int i=2,len=sheet2.getLastRowNum();i<len;i++) {
			HSSFRow row = sheet2.getRow(i);
			 
			HSSFCell cell0 = row.getCell(0);
			String key = cell0.getStringCellValue();
			 
			HSSFCell cell1 = row.getCell(1);
			Double value = cell1.getNumericCellValue();
			
			ModelBo mb = new ModelBo();
			
			if (map.containsKey(key)) {
				mb = map.get(key);
			}
			mb.setAge2(value);
			
			map.put(key, mb);
		}
		
		HSSFWorkbook workbook = new HSSFWorkbook();
		HSSFSheet sheet = workbook.createSheet("test");
		
		int num = 0;
		for (String str : map.keySet()) {
			
			HSSFRow row = sheet.createRow(num);
			row.createCell(0).setCellValue(str);
			
			ModelBo mb = map.get(str);
			
			row.createCell(1).setCellValue((mb.getAge()==null) ? 0.0 : mb.getAge());
			row.createCell(2).setCellValue((mb.getAge2()==null) ? 0.0 : mb.getAge2());
			
			num++;
		}
		
		workbook.write(out);
	}
	
}
