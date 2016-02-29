package edu.umass.cs.gnsserver.activecode.scratch;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;


public class TestCancelTask {
	
	static class Task implements Runnable {
		
		public void run() {
			System.out.println("Task running");
			throw new RuntimeException("TaskException " + System.currentTimeMillis());
		}
	}
	
	public static void main(String[] args) throws InterruptedException, ExecutionException{
		ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(4);
		Task task = new Task();
		Future<?> future  = executor.submit(task);
		Thread.sleep(1000);
		try {
			future.get();
		} catch(ExecutionException re) {
			re.printStackTrace();
			future.get();
		}
	}
}
