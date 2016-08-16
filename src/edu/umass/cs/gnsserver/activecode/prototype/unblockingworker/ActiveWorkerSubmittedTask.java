package edu.umass.cs.gnsserver.activecode.prototype.unblockingworker;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Channel;

/**
 * @author gaozy
 *
 */
public class ActiveWorkerSubmittedTask implements Runnable {
	
	final ThreadPoolExecutor executor;
	final ActiveRunner runner;
	final ActiveMessage request;
	final Channel channel;
	final ConcurrentHashMap<Long, ActiveRunner> map;
	
	ActiveWorkerSubmittedTask(ThreadPoolExecutor executor, ActiveRunner runner, ActiveMessage request, 
			Channel channel, ConcurrentHashMap<Long, ActiveRunner> map){
		this.executor = executor;
		this.runner = runner;
		this.request = request;
		this.channel = channel;
		this.map = map;
	}
	
	@Override
	public void run() {
		ActiveMessage response = null;
		long timeout = request.getBudget();
		
		Future<ActiveMessage> future = executor.submit(new ActiveWorkerTask(runner, request));
		
		try {
			response = future.get(timeout, TimeUnit.MILLISECONDS);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			// return an error
			runner.release(null);
			response = new ActiveMessage(request.getId(), null, e.getMessage());			
		}
		
		try {
			channel.sendMessage(response);
		} catch (IOException e) {
			throw new RuntimeException();
		}
		map.remove(request.getId());		
	}

}
