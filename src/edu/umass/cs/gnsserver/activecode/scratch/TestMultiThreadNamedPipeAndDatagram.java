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
	final static int totalReqs = 1000000;
	
	static class SingleDatagramClient implements Runnable {
		private DatagramSocket clientSocket;
		private int total;
		private final AtomicInteger received = new AtomicInteger();
		private int port;
		
		SingleDatagramClient(int port, int total){
			this.port = port;
			try {
				this.clientSocket = new DatagramSocket();
			} catch (SocketException e) {
				e.printStackTrace();
			}
			this.total = total;
		}
		
		@Override
		public void run() {	
			InetAddress localAddress = null;
			try {
				localAddress = InetAddress.getByName("localhost");
			} catch (UnknownHostException e1) {
				e1.printStackTrace();
			}
			byte[] b = new byte[length];
			new Random().nextBytes(b);
			DatagramPacket p = new DatagramPacket(b, b.length, localAddress, port);
			/**
			 * Send all requests sequentially
			 */
			while(received.get() < total){
				try {
					clientSocket.send(p);
					byte[] receiveData = new byte[length];
					DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
					clientSocket.receive(receivePacket);
					received.incrementAndGet();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
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
		}
		System.out.println("All clients ready!");
		
		
		long t = System.currentTimeMillis();
		
		for (int i=0; i<numServers; i++){
			executor.execute(clients[i]);
		}
		
		for(int i=0; i<numServers; i++){
			while(!clients[i].isFinished()){
				;
			}
		}
		
		long elapsed = System.currentTimeMillis() - t;
		System.out.println("It takes "+elapsed+"ms for Datagram socket");
		System.out.println("The average throughput with "+numServers+" Datagram servers and clients is "+numServers*reqsPerClient*1000.0/elapsed);
		
		executor.shutdown();		
	}
	
	
	static class PipeClient implements Runnable {
		ActiveChannel channel;
		final int total;
		private final AtomicInteger counter = new AtomicInteger();
		
		PipeClient(String ifile, String ofile, int total){
			this.total = total;
			channel = new ActivePipe(ifile, ofile);			
		}
		
		@Override
		public void run() {
			byte[] buffer = new byte[length];
			
			
			while( counter.get() < total ){
				if(channel.read(buffer) >0){
					counter.incrementAndGet();
				}
			}
		}
		
		public boolean isFinished(){
			return counter.get() == total;
		}
		
		public void sendData(byte[] buf){
			channel.write(buf, 0, buf.length);
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
	 * 
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
		
		executor.shutdown();	
	}
	
	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		numServers = 1; //Integer.parseInt(args[0]);
		
		Result result = JUnitCore.runClasses(TestMultiThreadNamedPipeAndDatagram.class);
		for (Failure failure : result.getFailures()) {
			System.out.println(failure.getMessage());
			failure.getException().printStackTrace();
		}
		System.exit(0);
	}
}
