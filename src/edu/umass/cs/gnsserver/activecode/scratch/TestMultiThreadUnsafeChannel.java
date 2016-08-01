package edu.umass.cs.gnsserver.activecode.scratch;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import io.mappedbus.MappedBusReader;
import io.mappedbus.MappedBusWriter;

/**
 * @author gaozy
 *
 */
public class TestMultiThreadUnsafeChannel {
	
	private MappedBusReader reader;
	private MappedBusWriter writer;
	
	byte[] buffer = new byte[msgSize];
	
	// The size of the random access file is 1mB
	final static long fullSize = 1000000000L;
	
	// The maximal size of the message is 4KB
	final static int msgSize = 128;
	
	/**
	 * @param rfile
	 * @param wfile
	 */
	public TestMultiThreadUnsafeChannel(String rfile, String wfile){
		reader = new MappedBusReader(rfile, fullSize, msgSize);
		writer = new MappedBusWriter(wfile, fullSize, msgSize, true);
		
		try {
			reader.open();
			writer.open();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private byte[] read() throws EOFException{
		byte[] buf = null;
		if(reader.next()){
			buf = new byte[msgSize];
			reader.readBuffer(buf, 0);
		}
		return buf;
	}
	
	private void write(byte[] buf){
		try {
			writer.write(buf, 0, buf.length);
		} catch (EOFException e) {
			e.printStackTrace();
		}
	}
	
	
	
	static class MappedBusClient implements Runnable {
		final int total;
		private final AtomicInteger counter = new AtomicInteger();
		//private final AtomicLong timeSpent = new AtomicLong();
		TestMultiThreadUnsafeChannel channel;
		
		MappedBusClient(String ifile, String ofile, int total){
			this.total = total;
			channel = new TestMultiThreadUnsafeChannel(ifile, ofile);		
		}
		
		@Override
		public void run() {
			
			while( counter.get() < total ){
				try {
					byte[] buf;
					if(( buf = channel.read() )!= null){
						//timeSpent.addAndGet(System.nanoTime());
						//System.out.println("receive:"+new String(buf));
						counter.incrementAndGet();					
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		public boolean isFinished(){
			return counter.get() == total;
		}
		
		public void sendMessage(byte[] buf){
			///timeSpent.addAndGet(-System.nanoTime());			
			channel.write(buf);
			
		}
		
		public double getLatnecy(){
			//return timeSpent.get()/((double) total);
			return 0.0;
		}
	}
	
	static class MappedBusServer implements Runnable {
		
		TestMultiThreadUnsafeChannel channel;
		
		MappedBusServer(String ifile, String ofile){
			channel = new TestMultiThreadUnsafeChannel(ifile, ofile);
		}
		
		@Override
		public void run() {
			while(true){
				try {
					byte[] buf = channel.read();
					if(buf != null){
						channel.write(buf);
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int total = 1000000;
		
		String suffix = "_test";
		String cfile = "/tmp/client"+suffix;
		String sfile = "/tmp/server"+suffix;
		Random random = new Random();
		
		try{
			
			MappedBusServer server = new MappedBusServer(cfile, sfile);
			MappedBusClient client = new MappedBusClient(sfile, cfile, total);
			
			new Thread(server).start();
			new Thread(client).start();
			
			long t1 = System.currentTimeMillis();
			for(int i=0; i<total; i++){
				byte[] b = new byte[msgSize];
				random.nextBytes(b);
				//System.out.println("send:"+new String(b));
				client.sendMessage(b);
			}
			
			while(!client.isFinished())
				;
			
			long elapsed = System.currentTimeMillis() - t1;
			System.out.println("It takes "+elapsed+"ms, and the average latency for each operation is "+(elapsed*1000.0/total)+"us");
			System.out.println("The average throughput is "+(total*1000.0/elapsed));

			
		} catch (Exception e){
			
		} finally{		
			new File(cfile).delete();
			new File(sfile).delete();
		}
	}
	
}
