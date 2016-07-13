package edu.umass.cs.gnsserver.activecode.prototype.multithreading;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveNamedPipe;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveQuerier;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveRunner;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Channel;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Querier;
import edu.umass.cs.utils.DelayProfiler;

/**
 * @author gaozy
 *
 */
public class MultiThreadActiveWorker {
	
	protected final static int bufferSize = 1024;
	final ThreadPoolExecutor executor;
	final LinkedBlockingQueue<Runnable> queue;
	final AtomicInteger counter = new AtomicInteger();
	final ActiveRunner[] runners;
	private Channel channel;
	private Querier querier;
	private final int numThread;
	
	private final String ifile;
	private final String ofile;
	private final int id;
	
	protected MultiThreadActiveWorker(int numThread, String ifile, String ofile, int id){
		this.ifile = ifile;
		this.ofile = ofile;
		this.id = id;
		this.numThread = numThread;
		queue = new LinkedBlockingQueue<Runnable>();
		executor = new ThreadPoolExecutor(numThread, numThread, 0, TimeUnit.MILLISECONDS, queue);
		executor.prestartAllCoreThreads();		
		channel = new ActiveNamedPipe(ifile, ofile);
		querier = new ActiveQuerier(channel);
		
		runners = new ActiveRunner[numThread];
		
		for (int i=0; i<numThread; i++){
			runners[i] = new ActiveRunner(querier);
		}
		
		try {
			runWorker();
		} catch (IOException e) {
			e.printStackTrace();
		} finally{
			channel.shutdown();
		}
	}
	
	@Override
	public String toString(){
		return this.getClass().getSimpleName()+id;
	}
	
	protected void submitTask(ActiveMessage am){
		int k = counter.getAndIncrement();
		executor.execute(new MultiThreadActiveTask(runners[k%numThread], am, channel));
		/*
		if(k%10000 == 0){
			DelayProfiler.updateMovAvg("workerQueueSize", queue.size());
			System.out.println(DelayProfiler.getStats());
		}*/
	}
	
	private void runWorker() throws IOException {
		System.out.println("Start running "+this+" by listening on "+ifile+", and write to "+ofile);
		while(true){
			ActiveMessage request = null;
			if((request = (ActiveMessage) channel.receiveMessage()) != null){				
				submitTask(request);
			}
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args){
		int num = Integer.parseInt(args[0]);
		String cfile = args[1];
		String sfile = args[2];
		int id = Integer.parseInt(args[3]);
		new MultiThreadActiveWorker(num, cfile, sfile, id);
	}
}
