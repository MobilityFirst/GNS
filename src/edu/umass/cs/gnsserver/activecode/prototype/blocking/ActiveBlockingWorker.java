package edu.umass.cs.gnsserver.activecode.prototype.blocking;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage.Type;
import edu.umass.cs.gnsserver.activecode.prototype.channels.ActiveNamedPipe;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Channel;
import org.json.JSONException;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;


public class ActiveBlockingWorker {
	
	private static final Logger logger = Logger.getLogger(ActiveBlockingWorker.class.getName());
	
	private final ActiveBlockingRunner runner;
	
	private final Channel channel;
	private final int id;
	
	private final ThreadPoolExecutor executor;
	private final AtomicInteger counter = new AtomicInteger(0);		
	
	

	protected ActiveBlockingWorker(String ifile, String ofile, int id, int numThread) {
		this.id = id;
		
		channel = new ActiveNamedPipe(ifile, ofile);
		runner = new ActiveBlockingRunner(channel);
		
		executor = new ThreadPoolExecutor(numThread, numThread, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		executor.prestartAllCoreThreads();	
		
		try {
			runWorker();
		} catch (JSONException | IOException e) {
			ActiveBlockingWorker.getLogger().log(Level.WARNING, "{0} catch an exception {1} and terminiates.", new Object[]{this, e});
		}finally{
			// close the channel and exit
			channel.close();
		}
		
	}

	
	private void runWorker() throws JSONException, IOException {
		
		
		while(!Thread.currentThread().isInterrupted()){
			ActiveMessage msg = null;
			if((msg = (ActiveMessage) channel.receiveMessage()) != null){

				if(msg.type == Type.REQUEST){
					ActiveMessage response = null;
					Future<ActiveMessage> future = executor.submit(new ActiveWorkerBlockingTask(runner, msg));
					try {
						response = future.get(msg.getBudget(), TimeUnit.MILLISECONDS);
					} catch (InterruptedException | ExecutionException | TimeoutException e) {
						// e.printStackTrace();
						// construct a response with an error and cancel this task
						future.cancel(true);
						response = new ActiveMessage(msg.getId(), null, e.getMessage());
					}
					// send back response
					channel.sendMessage(response);
					counter.getAndIncrement();
				}			
			}else{
				// The client is shutdown, let's exit this loop and return
				break;
			}
		}
	}
	
	
	public String toString(){
		return this.getClass().getSimpleName()+id;
	}
	

	protected static Logger getLogger(){
		return logger;
	}
	

	public static void main(String[] args){
		boolean pipeEnable = Boolean.parseBoolean(args[4]);
		if(pipeEnable){
			String cfile = args[0];
			String sfile = args[1];
			int id = Integer.parseInt(args[2]);
			int numThread = Integer.parseInt(args[3]);
			
			new ActiveBlockingWorker(cfile, sfile, id, numThread);
		}
	}
}
