package edu.umass.cs.gnsserver.activecode.prototype.multithreading;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.json.JSONException;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage;
import edu.umass.cs.gnsserver.activecode.prototype.ActivePipe;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveQuerier;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveRunner;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.ActiveChannel;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Querier;

/**
 * @author gaozy
 *
 */
public class MultiThreadActiveWorker {
	
	protected final static int bufferSize = 1024;
	final ThreadPoolExecutor executor;
	final AtomicInteger counter = new AtomicInteger();
	final ActiveRunner[] runners;
	private ActiveChannel channel;
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
		
		executor = new ThreadPoolExecutor(numThread, numThread, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		executor.prestartAllCoreThreads();		
		channel = new ActivePipe(ifile, ofile);
		querier = new ActiveQuerier(channel);
		
		runners = new ActiveRunner[numThread];
		
		for (int i=0; i<numThread; i++){
			runners[i] = new ActiveRunner(querier);
		}
		
		try {
			runWorker();
		} catch (UnsupportedEncodingException | JSONException e) {
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
		executor.execute(new MultiThreadActiveTask(runners[counter.getAndIncrement()%numThread], am, channel));
	}
	
	protected synchronized void sendResponse(byte[] buffer){
		channel.write(buffer, 0, buffer.length);
	}
	
	private void runWorker() throws UnsupportedEncodingException, JSONException {		
		byte[] buffer = new byte[bufferSize];
		System.out.println("Start running "+this+" by listening on "+ifile+", and write to "+ofile);
		while(true){
			if(channel.read(buffer) > 0){
				ActiveMessage msg = new ActiveMessage(buffer);
				//System.out.println("Length:"+length+",Type:"+msg.type+",msg:"+msg);
				Arrays.fill(buffer, (byte) 0);			
				submitTask(msg);
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
