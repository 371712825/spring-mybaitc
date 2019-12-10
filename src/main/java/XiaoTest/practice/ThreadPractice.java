package XiaoTest.practice;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** 
* @author Lusx 
* @date 2019年8月19日 上午11:42:51 
*/
public class ThreadPractice {

	public class ThreadSub implements Runnable {

		private String taskname;
		
		public ThreadSub(int i) {
			this.setName(i);
			this.taskname = "*"+i;
		}
		
		private void setName(Integer i) {
			Thread.currentThread().setName("ThreadSub-"+i);
		}
		
		@Override
		public void run() {
			// TODO Auto-generated method stub
			System.out.println(Thread.currentThread().getName()+">>>>>"+this.taskname);
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	private class ThreadCall implements Callable{

		private String taskname;
		
		public ThreadCall(int i) {
			this.taskname = "&&"+i;
			Thread.currentThread().setName(taskname);
		}
		
		@Override
		public String call() throws Exception {
			// TODO Auto-generated method stub
			return this.taskname;
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		
		BlockingQueue<Runnable> bq = new ArrayBlockingQueue<Runnable>(2);
		
//		bq.put(null);
//		bq.take();
		
		//ExecutorService es = Executors.newSingleThreadExecutor();
		ThreadPoolExecutor tpe = new ThreadPoolExecutor(2, 3, 5000, TimeUnit.MILLISECONDS, bq);
		
		for (int i=0;i<5;i++) {
			ThreadSub sub = new ThreadPractice().new ThreadSub(i);
			//ThreadCall call = new ThreadPractice().new ThreadCall(i);
			tpe.submit(sub);
		}
		
		
		tpe.shutdown();
		
		
	}
	
}
