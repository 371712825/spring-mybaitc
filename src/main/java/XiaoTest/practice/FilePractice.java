package XiaoTest.practice;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;



public class FilePractice {

	public static void main(String[] args) throws Exception{
		
		//用于解析上报数据-普通版	无法解析的用python版
		/*String str = "\\x84\\xAAproperties\\x98\\x87\\xACclose_reason";
		JSONObject re = MsgPackUtils.getDeserializedJSONObject(str);
		System.out.println(re);*/
		//String aa = re.toJSONString();
		//System.out.println(aa);
		
		
		
		//用于整合多天数据合成一张表
        /*File directory = new File("G:\\妈妈网\\BI\\数据整合\\uv");
        File[] files = directory.listFiles();
        
        if (files.length > 0) {
        	BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream("G:\\妈妈网\\BI\\数据整合\\test.csv"));
            
            for(int i = 0;i<files.length;i++){
                if(files[i].getName().endsWith("csv")){//是.xls文件时才执行
                    File file = new File(directory.getCanonicalPath() + "\\" + files[i].getName());
                    BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
                    
                    byte[] bytes = new byte[2048];
                    int n = -1;
                    while ((n = in.read(bytes,0,bytes.length)) != -1) {
                        
                        out.write(bytes, 0, n);
                    }
                    in.close();
                }
            }
            out.flush();
            out.close();
            System.out.println("success!!!*******");
        }*/

		//用于整合多天数据-聚合
		/*File uv = new File("G:\\妈妈网\\BI\\数据整合\\uv.xls");
		FileInputStream input = null; 
		FileOutputStream output = null;
		
		try {
			input = new FileInputStream(uv);
			HSSFWorkbook workBook = new HSSFWorkbook(input);
			HSSFSheet sheet = workBook.getSheetAt(0);
			Map<String,Double> map = new HashMap<>();
			
			int sheetLength = sheet.getPhysicalNumberOfRows();
			
			for (int i=3; i<sheetLength; i++) {
				HSSFCell keyCell = sheet.getRow(i).getCell(0);
				HSSFCell valueCell = sheet.getRow(i).getCell(1);
				
				String k = String.valueOf((int)keyCell.getNumericCellValue());
				Double v = valueCell.getNumericCellValue();
				if (map.containsKey(k)) {
					map.put(k, map.get(k) + v);
				} else {
					map.put(k, v);
				}
			}
			
			int index = 3;
			for (String k : map.keySet()) {
				HSSFCell keyCellOut = sheet.getRow(index).createCell(5);
				HSSFCell valueCellOut = sheet.getRow(index).createCell(6);
				
				keyCellOut.setCellValue(k);
				valueCellOut.setCellValue(map.get(k));
				index++;
				output = new FileOutputStream("G:\\\\妈妈网\\\\BI\\\\数据整合\\\\uvTest.xls");  
                workBook.write(output);
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				input.close();
				output.close();
			} catch(Exception e) {
				System.out.println("error");
			}
		}*/
		
		/*File uv = new File("G:\\妈妈网\\BI\\数据整合\\uvTest.xls");
		File info = new File("G:\\妈妈网\\BI\\数据整合\\info.xls");
		FileInputStream uvInput = null; 
		FileInputStream infoInput = null; 
		FileOutputStream output = null;
		
		try {
			uvInput = new FileInputStream(uv);
			infoInput = new FileInputStream(info);
			
			HSSFWorkbook workBook = new HSSFWorkbook(uvInput);
			HSSFSheet sheet = workBook.getSheetAt(0);
			Map<String,Double> map = new HashMap<>();
			int sheetLength = sheet.getPhysicalNumberOfRows();
			
			HSSFWorkbook workBook2 = new HSSFWorkbook(infoInput);
			HSSFSheet sheet2 = workBook2.getSheetAt(0);
			int sheetLength2 = sheet2.getPhysicalNumberOfRows();
			
			for (int i=3; i<sheetLength; i++) {
				HSSFCell keyCell = sheet.getRow(i).getCell(0);
				HSSFCell valueCell = sheet.getRow(i).getCell(1);
				
				String k = String.valueOf((int)keyCell.getNumericCellValue());
				Double v = valueCell.getNumericCellValue();
				if (map.containsKey(k)) {
					map.put(k, map.get(k) + v);
				} else {
					map.put(k, v);
				}
			}
			
			for (int i=3; i<sheetLength2; i++) {
				HSSFCell keyCell = sheet2.getRow(i).getCell(0);
				HSSFCell valueCell = sheet2.getRow(i).createCell(5);
				
				Double v = 0.0;
				String k = String.valueOf((int)keyCell.getNumericCellValue());
				if (map.containsKey(k)) {
					v = map.get(k);
				}
				
				valueCell.setCellValue(v);
				
				output = new FileOutputStream("G:\\\\妈妈网\\\\BI\\\\数据整合\\\\allTest.xls");  
                workBook2.write(output);
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				uvInput.close();
				output.close();
			} catch(Exception e) {
				System.out.println("error");
			}
		}*/
		
		/*File uv = new File("G:\\妈妈网\\BI\\数据整合\\allTest.xls");
		FileInputStream uvInput = null; 
		FileOutputStream output = null;
		
		try {
			uvInput = new FileInputStream(uv);
			
			HSSFWorkbook workBook = new HSSFWorkbook(uvInput);
			HSSFSheet sheet = workBook.getSheetAt(0);
			int sheetLength = sheet.getPhysicalNumberOfRows();
			
			for (int i=3; i<sheetLength; i++) {
				HSSFCell valueCell = sheet.getRow(i).getCell(1);
				String v = valueCell.getStringCellValue();
				
				if ("汇总".equals(v)) {
					sheet.removeRow(sheet.getRow(i));
					//sheet.shiftRows(i, i, -1);
					//System.out.println(i);
				}
				
				output = new FileOutputStream("G:\\\\妈妈网\\\\BI\\\\数据整合\\\\infoAndUv.xls");  
                workBook.write(output);
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				uvInput.close();
				output.close();
			} catch(Exception e) {
				System.out.println("error");
			}
		}*/
		
		
	}
	
	
	//测试用
	/*public class runTask implements Runnable {
		
		
		public void run() {
			try {
				System.out.println(Thread.currentThread());
				
			} catch (Exception e) {
				
			} 
		}
	}
	
	public class callTask implements Callable<Object> {
		
		@Override
		public Object call() {
			try {
				System.out.println(Thread.currentThread());
			} catch (Exception e) {
				
			} 
			return null;
		}
		
	}*/
	
}
