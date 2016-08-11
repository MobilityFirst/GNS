package edu.umass.cs.gnsserver.activecode.scratch;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
	final static int length = 128;
	final static int totalReqs = 100000;
	
	
	static class CircularChannel {
		CircularBufferedRandomAccessFile reader;
		CircularBufferedRandomAccessFile writer;
		
		CircularChannel(String ifile, String ofile, String id){
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
			channel = new CircularChannel(ifile, ofile, "Server");
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
					//System.out.println("Server received:"+(new String(data)));
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
		private final ArrayList<Long> sent = new ArrayList<Long>();
		private final ArrayList<Long> received = new ArrayList<Long>();
		
		CircularClient(String ifile, String ofile, int total){
			this.total = total;
			channel = new CircularChannel(ifile, ofile, "Client");			
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
					//received.add(System.nanoTime());
					counter.incrementAndGet();	
					//System.out.println("Client received:"+new String(data));
				}
			}
		}
		
		public boolean isFinished(){
			//System.out.println(counter.get());
			return counter.get() == total;
		}
		
		public void sendData(byte[] buf){
			//sent.add(System.nanoTime());
			try {
				channel.write(buf);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		public double getLatnecy(){
			long sum =0;
			for(long lat:received){
				sum += lat;
			}
			for(long lat:sent){
				sum -= lat;
			}
			return sum/((double) total);
		}
		
		public void close(){
			channel.close();
		}
	}
	
	/**
	 * @throws InterruptedException 
	 * 
	 */
	@Test
	public void test_ParallelCircularChannelThroughput() throws InterruptedException{
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
				byte[] buffer = ("hello world!"+k).getBytes();
				clients[i].sendData(buffer);
			}
		}
				
		for(int i=0; i<numServers; i++){
			while(!clients[i].isFinished()){
				//Thread.sleep(1000);
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
		// System.out.println("The average latency is "+lat/1000.0/numServers+"us");
		// executor.shutdown();
		
		for(int i=0; i<numServers; i++){
			new File(cfile+i).delete();
			new File(sfile+i).delete();
		}
		
	}
	
	/**
	 * This test shows that the byte being read by {@code RandomAccessFile} will
	 * not be reset. 
	 * @throws IOException 
	 */
	//@Test
	public void test_RandomAccessFileRead() throws IOException{
		RandomAccessFile raf = new  RandomAccessFile(new File("/tmp/test"),"rw");
		for(int i=0; i<10; i++){
			byte[] buf = ("hello world "+i).getBytes();
			raf.write(buf, 0, buf.length);
		}
		
		raf.seek(0);
		byte[] buffer = new byte[13];
		raf.read(buffer);
		System.out.println("The first byte is "+new String(buffer));
		
		raf.seek(0);
		
		buffer = new byte[13];
		raf.read(buffer);
		System.out.println("The first byte is "+new String(buffer));
		
		raf.close();
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
