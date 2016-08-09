package edu.umass.cs.gnsserver.activecode.scratch;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import edu.umass.cs.gnsserver.activecode.prototype.utils.CircularBufferedRandomAccessFile;

/**
 * This test is used for testing ActiveCircularChannel.
 * 
 * @author gaozy
 *
 */
public class TestCircularChannel {
	private static int numServers = 1;
	final static int length = 512;
	final static int totalReqs = 1000000;
	
	
	static class CircularChannel {
		CircularBufferedRandomAccessFile reader;
		CircularBufferedRandomAccessFile writer;
		
		CircularChannel(String ifile, String ofile){
			reader = new CircularBufferedRandomAccessFile(ifile);
			writer = new CircularBufferedRandomAccessFile(ofile);
		}
		
		byte[] read() throws IOException{
			return reader.read();
		}
		
		void write(byte[] data) throws IOException{
			writer.write(data);
		}
		
		void close(){
			reader.shutdown();
			writer.shutdown();
		}
	}
	
	
	static class CircularServer implements Runnable{
		CircularChannel channel;
		
		CircularServer(String ifile, String ofile){
			channel = new CircularChannel(ifile, ofile);
		}
		
		@Override
		public void run() {
			while(!Thread.currentThread().isInterrupted()){
				byte[] data = null;
				try {
					data = channel.read();
				} catch (IOException e) {
					e.printStackTrace();
				}
				if(data != null){
					try {
						channel.write(data);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
		
		public void close(){
			channel.close();
		}
	}
	
	static class CircularClient implements Runnable{
		
		CircularChannel channel;
		final int total;
		private final AtomicInteger counter = new AtomicInteger();
		private final AtomicLong timeSpent = new AtomicLong();
		
		CircularClient(String ifile, String ofile, int total){
			this.total = total;
			channel = new CircularChannel(ifile, ofile);			
		}
		
		@Override
		public void run() {
					
			while( counter.get() < total ){
				byte[] data = null;
				try {
					data = channel.read();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if( data != null){
					timeSpent.addAndGet(System.nanoTime());
					counter.incrementAndGet();					
				}
			}
		}
		
		public boolean isFinished(){
			return counter.get() == total;
		}
		
		public void sendData(byte[] buf){
			timeSpent.addAndGet(-System.nanoTime());
			try {
				channel.write(buf);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		public double getLatnecy(){
			return timeSpent.get()/((double) total);
		}
		
		public void close(){
			channel.close();
		}
	}
	
	/**
	 * 
	 */
	@Test
	public void test_ParallelCircularChannelThroughput(){
		final String cfile = "/tmp/client";
		final String sfile = "/tmp/server";
		
		byte[] buf = new byte[length];
		new Random().nextBytes(buf);
		
		final ThreadPoolExecutor executor = new ThreadPoolExecutor(numServers*2, numServers*2, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		executor.prestartAllCoreThreads();
		int reqsPerClient = totalReqs/numServers;
		
		CircularServer[] servers = new CircularServer[numServers];
		// start all servers
		new Thread(){
			@Override
			public void run(){
				for (int i=0; i<numServers; i++){
					servers[i] = new CircularServer(sfile+i, cfile+i);
					executor.execute(servers[i]);
				}
			}
		}.start();	
		
		CircularClient[] clients = new CircularClient[numServers];
		for (int i=0; i<numServers; i++){
			File f1 = new File(cfile+i);
			File f2 = new File(sfile+i);
			while(!f1.exists() || !f2.exists()) { 
			    ;
			}
			clients[i] = new CircularClient(cfile+i, sfile+i, reqsPerClient);
			executor.execute(clients[i]);
		}
		

		long t = System.currentTimeMillis();
		// start send all requests
		for (int k=0; k<reqsPerClient; k++){
			for (int i=0; i<numServers; i++){
				clients[i].sendData(buf);
			}
		}
				
		for(int i=0; i<numServers; i++){
			while(!clients[i].isFinished()){
				;
			}
		}
		
		long elapsed = System.currentTimeMillis() - t;
		System.out.println("It takes "+elapsed+"ms for parallel RandomAccesssFile.");
		System.out.println("The average throughput with "+numServers+" parallel named pipe servers and clients is "+numServers*reqsPerClient*1000.0/elapsed);
		double lat = 0;
		for (int i=0; i<numServers; i++) {
			lat += clients[i].getLatnecy();
			//System.out.println("The average latency is "+clients[i].getLatnecy());
		}
		//System.out.println("The average latency is "+lat/1000.0/numServers+"us");
		executor.shutdown();
		
		try {
			executor.awaitTermination(1, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		for(int i=0; i<numServers; i++){
			new File(cfile+i).delete();
			new File(sfile+i).delete();
		}
		
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args){
		
		Result result = JUnitCore.runClasses(TestCircularChannel.class);
		for (Failure failure : result.getFailures()) {
			System.out.println(failure.getMessage());
			failure.getException().printStackTrace();
		}
		System.exit(0);		
		
	}
}
