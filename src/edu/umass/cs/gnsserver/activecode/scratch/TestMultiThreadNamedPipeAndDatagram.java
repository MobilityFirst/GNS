package edu.umass.cs.gnsserver.activecode.scratch;

import java.io.File;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Arrays;
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

import edu.umass.cs.gnsserver.activecode.prototype.ActivePipe;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.ActiveChannel;


/**
 * @author gaozy
 *
 */
public class TestMultiThreadNamedPipeAndDatagram {
	
	private static int numServers = 1;
	final static int length = 1000;
	final static int totalReqs = 100000;
	
	static class SingleDatagramClient implements Runnable {
		//private List<Long> sendTime = Collections.synchronizedList(new ArrayList<Long>());
		//private List<Long> receiveTime = Collections.synchronizedList(new ArrayList<Long>());
		private DatagramSocket clientSocket;
		private int total;
		private final AtomicInteger received = new AtomicInteger();
		private final AtomicInteger sent = new AtomicInteger();
		private AtomicLong timeSpent = new AtomicLong();
		
		private int port;
		private Object obj = new Object();
		private boolean readyToSend = true;
		private int thres;
		
		
		SingleDatagramClient(int port, int total){
			this.port = port;
			try {
				this.clientSocket = new DatagramSocket();
			} catch (SocketException e) {
				e.printStackTrace();
			}
			this.total = total;
			
			try {
				// set threshold for traffic control
				thres = (int) (clientSocket.getSendBufferSize()/length*0.9);
			} catch (SocketException e) {
				e.printStackTrace();
				// otherwise use default number
				thres = 50;
			}
		}
		
		@Override
		public void run() {						
			/**
			 * receive all responses
			 */
			while(received.get() < total){
				try {
					byte[] receiveData = new byte[length];
					DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
					clientSocket.receive(receivePacket);
					timeSpent.getAndAdd(System.currentTimeMillis());
					received.getAndIncrement();
					
					if(sent.get() -received.get() < thres){
						readyToSend = true;
						synchronized(obj){
							obj.notifyAll();
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		public void sendAllrequests() {
			new Thread(){
				public void run(){
					InetAddress localAddress = null;
					try {
						localAddress = InetAddress.getByName("localhost");
					} catch (UnknownHostException e1) {
						e1.printStackTrace();
					}
					byte[] b = new byte[length];
					new Random().nextBytes(b);
					
					
					while(sent.get() < total){
						if(sent.get() - received.get() < thres){
							try {
								sent.incrementAndGet();
								timeSpent.addAndGet(-System.currentTimeMillis());
								//ByteBuffer.wrap(b).putLong(System.nanoTime());
								DatagramPacket packet = new DatagramPacket(b, b.length, localAddress, port);
								clientSocket.send(packet);								
							} catch (IOException e) {
								e.printStackTrace();
							}
						}else{
							readyToSend = false;
							synchronized(obj){
								while(!readyToSend){
									try {
										obj.wait();
									} catch (InterruptedException e) {
										e.printStackTrace();
									}
								}
							}
						}
					}
				}
			}.start();
		}
		
		public double getLatnecy(){
			return timeSpent.get()/((double) total);
		}
		
		public boolean isFinished(){
			return received.get() == total;
		}
		
	}
	
	static class SingleDatagramServer implements Runnable {
		
		private DatagramSocket serverSocket; 
		
		public SingleDatagramServer(int port){
			try {
				serverSocket = new DatagramSocket(port);
				//System.out.println("server's receive buffer size is:"+serverSocket.getReceiveBufferSize());
			} catch (SocketException e) {
				e.printStackTrace();
			}
		}
		
		@Override
		public void run() {
			byte[] receiveData = new byte[length];
			DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
			
			while(!Thread.currentThread().isInterrupted()){
				try {
					serverSocket.receive(receivePacket);
					DatagramPacket sendPacket = new DatagramPacket(receivePacket.getData(), receivePacket.getLength(), receivePacket.getAddress(), receivePacket.getPort());
					serverSocket.send(sendPacket);
				} catch (IOException e) {
					e.printStackTrace();
					break;
				}
			}
			
			serverSocket.close();
		}
		
	}
	
	/**
	 * @throws UnknownHostException 
	 * 
	 */
	@Test
	public void test_01_DatagramThroughput() throws UnknownHostException{
		final ThreadPoolExecutor executor = new ThreadPoolExecutor(numServers*2, numServers*2, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		executor.prestartAllCoreThreads();
		int port = 60001;
		int reqsPerClient = totalReqs/numServers;
		
		// start all servers
		for (int i=0; i<numServers; i++){
			executor.execute(new SingleDatagramServer(port+i));
		}
		System.out.println("All servers started!");
		
		// start all clients
		SingleDatagramClient[] clients = new SingleDatagramClient[numServers];
		for (int i=0; i<numServers; i++){
			clients[i] = new SingleDatagramClient(port+i, reqsPerClient);
			executor.execute(clients[i]);
		}
		System.out.println("All clients ready!");
		
		
		long t = System.currentTimeMillis();
		
		for (int i=0; i<numServers; i++){
			clients[i].sendAllrequests();
		}
		
		for(int i=0; i<numServers; i++){
			while(!clients[i].isFinished()){
				;
			}
		}
		
		long elapsed = System.currentTimeMillis() - t;
		System.out.println("It takes "+elapsed+"ms for Datagram socket");
		System.out.println("The average throughput with "+numServers+" Datagram servers and clients is "+numServers*reqsPerClient*1000.0/elapsed);
		double lat = 0;
		for (int i=0; i<numServers; i++) {
			lat += clients[i].getLatnecy();
			//System.out.println("The average latency is "+clients[i].getLatnecy());
		}
		System.out.println("The average latency is "+lat*1000.0/numServers+"us");
		
		executor.shutdown();		
	}
	
	
	static class PipeClient implements Runnable {
		//private List<Long> sendTime = Collections.synchronizedList(new ArrayList<Long>());
		//private List<Long> receiveTime = Collections.synchronizedList(new ArrayList<Long>());
		
		ActiveChannel channel;
		final int total;
		private final AtomicInteger counter = new AtomicInteger();
		private final AtomicLong timeSpent = new AtomicLong();
		
		PipeClient(String ifile, String ofile, int total){
			this.total = total;
			channel = new ActivePipe(ifile, ofile);			
		}
		
		@Override
		public void run() {
			byte[] buffer = new byte[length];
						
			while( counter.get() < total ){
				if(channel.read(buffer) >0){
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
			channel.write(buf, 0, buf.length);
		}
		
		public double getLatnecy(){
			return timeSpent.get()/((double) total);
		}
	}
	
	
	static class PipeServer implements Runnable{
		ActiveChannel channel;
		PipeServer(String ifile, String ofile){
			
			Runtime runtime = Runtime.getRuntime();
			try {
				runtime.exec("mkfifo "+ifile);
				runtime.exec("mkfifo "+ofile);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			
			channel = new ActivePipe(ifile, ofile);
		}
		
		@Override
		public void run() {
			byte[] buffer = new byte[length];
			int l =0;
			while(true){
				if((l=channel.read(buffer)) > 0){
					channel.write(buffer, 0, l);
					Arrays.fill(buffer, (byte) 0);
				}
			}
		}
		
	}
	
	/**
	 * Test the throughput of named pipe with the PipeClient and PipeServer as above
	 */
	@Test
	public void test_02_NamedPipeThroughput(){
		byte[] buf = new byte[length];
		new Random().nextBytes(buf);
		
		final ThreadPoolExecutor executor = new ThreadPoolExecutor(numServers*2, numServers*2, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		executor.prestartAllCoreThreads();
		int reqsPerClient = totalReqs/numServers;
		
		String cfile = "/tmp/client";
		String sfile = "/tmp/server";
		
		// start all servers
		new Thread(){
			@Override
			public void run(){
				for (int i=0; i<numServers; i++){
					executor.execute(new PipeServer(sfile+i, cfile+i));
				}
			}
		}.start();;
		System.out.println("All servers started!");
		
		
		
		// start all clients
		PipeClient[] clients = new PipeClient[numServers];
		for (int i=0; i<numServers; i++){
			File f1 = new File(cfile+i);
			File f2 = new File(sfile+i);
			while(!f1.exists() || !f2.exists()) { 
			    ;
			}
			clients[i] = new PipeClient(cfile+i, sfile+i, reqsPerClient);
			executor.execute(clients[i]);
		}
		System.out.println("All clients ready!");
		
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
		System.out.println("It takes "+elapsed+"ms for named pipe.");
		System.out.println("The average throughput with "+numServers+" named pipe servers and clients is "+numServers*reqsPerClient*1000.0/elapsed);
		double lat = 0;
		for (int i=0; i<numServers; i++) {
			lat += clients[i].getLatnecy();
			//System.out.println("The average latency is "+clients[i].getLatnecy());
		}
		System.out.println("The average latency is "+lat/1000.0/numServers+"us");
		
		executor.shutdown();	
	}
	
	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		numServers = Integer.parseInt(args[0]);
		
		Result result = JUnitCore.runClasses(TestMultiThreadNamedPipeAndDatagram.class);
		for (Failure failure : result.getFailures()) {
			System.out.println(failure.getMessage());
			failure.getException().printStackTrace();
		}
		System.exit(0);
	}
}
