package edu.umass.cs.gnsserver.activecode.scratch;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * @author gaozy
 *
 */
public class TestChainTask {
	static ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(4);
	static class Task implements Runnable {
		int id;
		Task(int id){
			this.id = id;
		}
		public void run() {
			
			//throw new RuntimeException("TaskException " + System.currentTimeMillis());
			if(id == 0){
				executor.submit(new Task(1));
			}
			while(!Thread.currentThread().isInterrupted()){
				System.out.println("Task "+ id +" running");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}
	
	static class CancelTask implements Runnable{
		Future<?> future;
		CancelTask(Future<?> future){
			this.future = future;
		}
		
		@Override
		public void run() {
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// TODO Auto-generated method stub
			future.cancel(true);
		}
		
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args){
		Task task = new Task(0);
		
		Future<?> future  = executor.submit(task);
		(new Thread(new CancelTask(future))).start();;
		try {
			future.get();
		} catch(ExecutionException re) {
			re.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
}
