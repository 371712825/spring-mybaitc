package XiaoTest.practice;

import java.net.InetSocketAddress;
import java.net.Socket;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool.Config;

public class CommonPoolPracticeFactory {

	private GenericObjectPool pool;  
	  
    public CommonPoolPracticeFactory(Config config,String ip,int port){  
        ConnectionFactory factory = new ConnectionFactory(ip, port);  
        pool = new GenericObjectPool(factory, config);  
    }  
      
    public Socket getConnection() throws Exception{  
        return (Socket)pool.borrowObject();  
    }  
      
    public void releaseConnection(Socket socket){  
        try{  
            pool.returnObject(socket);  
        }catch(Exception e){  
            if(socket != null){  
                try{  
                    socket.close();  
                }catch(Exception ex){  
                    //TODO
                	ex.printStackTrace(); 
                }  
            }  
        }  
    }  
      
    class ConnectionFactory extends BasePoolableObjectFactory {  
  
        private InetSocketAddress address;  
          
        public ConnectionFactory(String ip,int port){  
            address = new InetSocketAddress(ip, port);  
        }  
          
        @Override  
        public Object makeObject() throws Exception {  
            Socket socket = new Socket();  
            socket.connect(address);  
            return socket;  
        }  
          
        public void destroyObject(Object obj) throws Exception  {  
            if(obj instanceof Socket){  
                ((Socket)obj).close();  
            }  
        }  
  
        public boolean validateObject(Object obj) {  
            if(obj instanceof Socket){  
                Socket socket = ((Socket)obj);  
                if(!socket.isConnected()){  
                    return false;  
                }  
                if(socket.isClosed()){  
                    return false;  
                }  
                return true;  
            }  
            return false;  
        }  
  
    }  
}
