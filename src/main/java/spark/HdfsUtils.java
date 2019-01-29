package spark;
  
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsUtils {
	private static Logger logger = LoggerFactory.getLogger(HdfsUtils.class);
	
	public static String HOST = null;
	
    //initialization  
	private static Configuration conf = new Configuration();  
    static {
    	try {
    		conf.addResource(new Path("core-site.xml"));
            conf.addResource(new Path("hdfs-site.xml"));
            HOST = conf.get("fs.defaultFS");
    	} catch(Exception e){
    		logger.error(e.getMessage(), e);
    	}
    } 
      
    public static FileSystem getInstance() throws IOException {
        return FileSystem.get(conf);
    }
}  
