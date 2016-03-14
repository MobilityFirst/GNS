package edu.umass.cs.gnsserver.activecode.scratch;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class TestCancelWaitTask {
	static class Task implements Callable<String> {
		
		public String call() throws InterruptedException {
			System.out.println("Task running");
			try {
				synchronized(this){
					this.wait();
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return "";
		}
	}
	
	static class CancelThread implements Runnable{
		Future<String> future;
		
		CancelThread(Future<String> future){
			this.future = future;
		}
		
		@Override
		public void run() {
			// TODO Auto-generated method stub
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			future.cancel(true);
		}
		
	}
	
	public static void main(String[] args) {
		ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(4);
		Task task = new Task();
		Future<String> future  = executor.submit(task);
		
		(new Thread(new CancelThread(future))).start();
		
		try {
			future.get();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			System.out.println("this task has been interrupted");		
		} catch(ExecutionException re) {
			re.printStackTrace();
			
		} 
	}
}
