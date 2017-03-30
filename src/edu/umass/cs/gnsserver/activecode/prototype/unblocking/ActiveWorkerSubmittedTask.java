package edu.umass.cs.gnsserver.activecode.prototype.unblocking;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Channel;

/**
 * This class submits a task to a thread pool and time out the task.
 * If the task is timed out, it gets a TimeoutException and returns
 * a failure message to the client.
 * 
 * @author gaozy
 *
 */
public class ActiveWorkerSubmittedTask implements Runnable {
	
	final ThreadPoolExecutor executor;
	final ActiveNonBlockingRunner runner;
	final ActiveMessage request;
	final Channel channel;
	
	ActiveWorkerSubmittedTask(ThreadPoolExecutor executor, ActiveNonBlockingRunner runner, ActiveMessage request, 
			Channel channel){
		this.executor = executor;
		this.runner = runner;
		this.request = request;
		this.channel = channel;
	}
	
	@Override
	public void run() {
		ActiveMessage response = null;
		long timeout = request.getBudget();
		
		Future<ActiveMessage> future = executor.submit(new ActiveWorkerTask(runner, request));
		
		try {
			response = future.get(timeout, TimeUnit.MILLISECONDS);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			e.printStackTrace();
			// return an error
			response = new ActiveMessage(request.getId(), null, e.getMessage());
			ActiveNonBlockingWorker.getLogger().log(Level.FINE, 
					"get an exception {0} when executing request {1} with code {2}", 
					new Object[]{e, request, request.getCode()});
		}
		
		try {
			channel.sendMessage(response);
		} catch (IOException e) {
			throw new RuntimeException();
		}	
	}

}
