package XiaoTest.practice;

import java.net.Socket;

import org.apache.commons.pool.impl.GenericObjectPool.Config;

public class SocketPractice {

    public static void main(String[] args) {  
        Config config = new Config();  
        config.maxActive = 16;  
        config.maxWait = 30000;  
        CommonPoolPracticeFactory poolFactory = new CommonPoolPracticeFactory(config, "127.0.0.1", 8011);  
        Socket socket = null ;  
        try{  
            socket = poolFactory.getConnection();  
            //TODO
        }catch(Exception e){  
            e.printStackTrace();  
        }finally{  
            if(socket != null){  
                poolFactory.releaseConnection(socket);  
            }  
        }  
    } 
    
}
