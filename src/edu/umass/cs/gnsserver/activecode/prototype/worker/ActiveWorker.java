package edu.umass.cs.gnsserver.activecode.prototype.worker;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.json.JSONException;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveNamedPipe;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage.Type;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Channel;

/**
 * @author gaozy
 *
 */
public class ActiveWorker {
	
	
	private final ActiveRunner[] runners;
	
	private final Channel channel;
	private final int id;
	private final int numThread;
	
	private final ThreadPoolExecutor executor;
	private final ThreadPoolExecutor taskExecutor;
	private final ConcurrentHashMap<Long, ActiveRunner> map = new ConcurrentHashMap<Long, ActiveRunner>();
	private final AtomicInteger counter = new AtomicInteger(0);	
	
	
	
	/**
	 * Initialize a worker with a named pipe
	 * @param ifile
	 * @param ofile
	 * @param id 
	 * @param numThread 
	 * @param isTest
	 */
	protected ActiveWorker(String ifile, String ofile, int id, int numThread) {
		this.id = id;
		this.numThread = numThread;
		
		executor = new ThreadPoolExecutor(numThread, numThread, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		executor.prestartAllCoreThreads();
		taskExecutor = new ThreadPoolExecutor(numThread, numThread, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		taskExecutor.prestartAllCoreThreads();
		
		channel = new ActiveNamedPipe(ifile, ofile);
		runners = new ActiveRunner[numThread];
		
		for (int i=0; i<numThread; i++){
			runners[i] = new ActiveRunner(new ActiveQuerier(channel));
		}		

		try {
			runWorker();
		} catch (JSONException | IOException e) {
			e.printStackTrace();
			// close the channel and exit
		}finally{
			channel.shutdown();
		}
		
	}

	
	private void runWorker() throws JSONException, IOException {	
		while(!((ActiveNamedPipe) channel).getReady())
			;
		
		ActiveMessage msg = null;
		while(!Thread.currentThread().isInterrupted()){
			if((msg = (ActiveMessage) channel.receiveMessage()) != null){
				if(msg.type == Type.REQUEST){
					ActiveRunner runner = runners[counter.getAndIncrement()%numThread];
					map.put(msg.getId(), runner);
					taskExecutor.submit(new ActiveWorkerSubmittedTask(executor, runner, msg, channel, map));
					
				} else if (msg.type == Type.RESPONSE ){
					map.get(msg.getId()).release(msg);
				}
			}else{
				// The client is shutdown
				break;
			}
		}
	}
	
	
	public String toString(){
		return this.getClass().getSimpleName()+id;
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args){
		boolean pipeEnable = Boolean.parseBoolean(args[4]);
		if(pipeEnable){
			String cfile = args[0];
			String sfile = args[1];
			int id = Integer.parseInt(args[2]);
			int numThread = Integer.parseInt(args[3]);
			
			new ActiveWorker(cfile, sfile, id, numThread);
		}
	}
}
