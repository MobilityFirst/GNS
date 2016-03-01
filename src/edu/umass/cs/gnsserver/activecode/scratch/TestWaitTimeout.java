package edu.umass.cs.gnsserver.activecode.scratch;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class TestWaitTimeout {
	
	static class Task implements Callable<String> {
		private boolean flag = false;
		private Object monitor = new Object();
		public String call() {
			if(!flag){
				synchronized(monitor){
					try {
						monitor.wait(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
			}
			return "here";
		}
	}
	
	public static void main(String[] args) throws InterruptedException, ExecutionException{
		ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(4);
		FutureTask<String> task = new FutureTask<String>(new Task());
		executor.execute(task);
		//Thread.sleep(1000);
		String result = "there";
		try {
			result = task.get();
		} catch(ExecutionException re) {
			re.printStackTrace();
		} catch (Exception e){ 
			e.printStackTrace();
		}
		System.out.println("result is "+result);
		
		System.exit(0);
	}
}
