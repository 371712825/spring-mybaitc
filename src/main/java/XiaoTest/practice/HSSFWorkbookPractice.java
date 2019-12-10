package XiaoTest.practice;

import java.io.File;
import java.io.FileInputStream;

import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

/** 
* @author Lusx 
* @date 2019年5月29日 下午12:02:08 
*/
public class HSSFWorkbookPractice {

	public static void main(String[] args) throws Exception {
		
		FileInputStream fis = new FileInputStream(new File("G:\\\\用户核心行为验证-0528-1.xls"));
		HSSFWorkbook hw = new HSSFWorkbook(fis);
		
		HSSFSheet sheet = hw.getSheet("3月-怀孕");
		HSSFRow row = sheet.getRow(2);
		
		System.out.println(row.getCell(0).getStringCellValue());
	}
}
