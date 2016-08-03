package edu.umass.cs.gnsserver.activecode.prototype.multithreading;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage.Type;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveNamedPipe;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveQuerier;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveRunner;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Channel;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Querier;

/**
 * @author gaozy
 *
 */
public class MultiThreadActiveWorker implements Runnable {
	
	protected final static int bufferSize = 1024;
	final ThreadPoolExecutor executor;
	final LinkedBlockingQueue<Runnable> queue;
	final AtomicInteger counter = new AtomicInteger();
	final ActiveRunner[] runners;
	private Channel channel;
	private Querier[] queriers;
	private final int numThread;
	
	private final String ifile;
	private final String ofile;
	private final int id;
	
	protected AtomicInteger numReq = new AtomicInteger();
	
	protected MultiThreadActiveWorker(int numThread, String ifile, String ofile, int id){
		this.ifile = ifile;
		this.ofile = ofile;
		this.id = id;
		this.numThread = numThread;
		queue = new LinkedBlockingQueue<Runnable>();
		
		executor = new ThreadPoolExecutor(numThread, numThread, 0, TimeUnit.MILLISECONDS, queue);
		executor.prestartAllCoreThreads();
		
		channel = new ActiveNamedPipe(ifile, ofile);
		
		
		runners = new ActiveRunner[numThread];
		
		for (int i=0; i<numThread; i++){
			Querier querier = new MultiThreadActiveQuerier(channel);
			queriers[i] = querier;
			runners[i] = new ActiveRunner(querier);
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
	
	@Override
	public void run(){
		
		System.out.println("Start running "+this+" by listening on "+ifile+", and write to "+ofile);
		while(!Thread.currentThread().isInterrupted()){
			ActiveMessage message = null;
			try {
				if((message = (ActiveMessage) channel.receiveMessage()) != null){
					if(message.type == Type.REQUEST){
						submitTask(message);
					}else{
						if(message.type == Type.RESPONSE){
							// invoke the querier here
						}
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
				break;
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
		new Thread(new MultiThreadActiveWorker(num, cfile, sfile, id)).start();
	}
}
