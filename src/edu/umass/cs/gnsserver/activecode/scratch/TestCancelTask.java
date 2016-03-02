package edu.umass.cs.gnsserver.activecode.scratch;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;


public class TestCancelTask {
	
	static class Task implements Callable<String> {
		
		public String call() throws InterruptedException {
			System.out.println("Task running");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			throw new InterruptedException("interrupted");
			
		}
	}
	
	public static void main(String[] args) {
		ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(4);
		Task task = new Task();
		Future<?> future  = executor.submit(task);
		
		try {
			future.get();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			System.out.println("this task has been interrupted");		
		} catch(ExecutionException re) {
			re.printStackTrace();
			//future.get();
		}
	}
}
