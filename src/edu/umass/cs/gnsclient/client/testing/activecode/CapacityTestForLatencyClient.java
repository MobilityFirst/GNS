package edu.umass.cs.gnsclient.client.testing.activecode;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import edu.umass.cs.gigapaxos.paxosutil.RateLimiter;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Util;

/**
 * @author gaozy
 *
 */
public class CapacityTestForLatencyClient{
	
	final static Random random = new Random();
	
	/**
	 * Test for 1 min
	 */
	private final static int DURATION = 60*1000;
	private static int NUM_THREAD = 20;
	
	private static int numClients;
	private static String someField = "someField";
	private static String someValue = "someValue";
	private static boolean withSignature; 	
	private static int RATE;
	private static String resultFile;
	private static int TOTAL;
	private static boolean isRead;
	private static int EXTRA_WAIT_TIME; // second
	private static GuidEntry entry;
	private static GuidEntry[] entries;
	private static int guidIndex;
	private static int numGuids;
	
	private static GNSClientCommands[] clients;
	private static boolean sequential = false;
	
	private static ExecutorService executor;
	
	private static int received = 0;
	private static long elapsed = 0;
	static synchronized void increaseLatency(long lat){
		received++;
		elapsed += lat;
	}
	static synchronized void reset(){
		received = 0;
		elapsed = 0;
	}
	static int getRcvd(){
		return received;
	}
	
	/**
	 * @throws Exception
	 */
	public static void setup() throws Exception {
		numClients = 10;
		if(System.getProperty("numClients") != null){
			numClients = Integer.parseInt(System.getProperty("numClients"));
		}
		System.out.println("There are "+numClients+" clients.");
		
		someField = "someField";
		if(System.getProperty("field")!=null){
			someField = System.getProperty("field");
		}
		
		withSignature = false;
		if(System.getProperty("withSigniture")!= null){
			withSignature = Boolean.parseBoolean(System.getProperty("withSigniture"));
		}
		
		RATE = 10;
		if(System.getProperty("rate")!=null){
			RATE = Integer.parseInt(System.getProperty("rate"));
		}
		TOTAL = RATE*DURATION/1000;
		
		EXTRA_WAIT_TIME = 0;
		if(System.getProperty("extraTime") != null){
			EXTRA_WAIT_TIME = 1000*Integer.parseInt(System.getProperty("extraTime"));
		}
		
		isRead = true;
		if(System.getProperty("isRead")!=null){
			isRead = Boolean.parseBoolean(System.getProperty("isRead"));
		}
		
		if(System.getProperty("numThread")!=null){
			NUM_THREAD = Integer.parseInt(System.getProperty("numThread"));
		}
		
		if(System.getProperty("sequential")!=null){
			sequential = Boolean.parseBoolean(System.getProperty("sequential"));
		}
		
		String keyFile = "guid";
		if(System.getProperty("keyFile")!= null){
			keyFile = System.getProperty("keyFile");
		}
		ObjectInputStream input = new ObjectInputStream(new FileInputStream(new File(keyFile)));
		entry = new GuidEntry(input);
		assert(entry != null);
		
		executor = Executors.newFixedThreadPool(NUM_THREAD);
		
		clients = new GNSClientCommands[numClients];
		for (int i=0; i<numClients; i++){
			clients[i] = new GNSClientCommands();
		}
	}
	
	/**
	 * @throws InterruptedException 
	 * @throws FileNotFoundException 
	 * 
	 */	
	public static void latency_test() throws FileNotFoundException, InterruptedException{
		System.out.println("Start running experiment for "+(withSignature?"signed":"unsigned")+" "+(isRead?"read":"write"));
		executor.submit(new SingleGNSClientTask(clients[0], entry, ((Integer) RATE).doubleValue(), TOTAL, true));
		
		try {
			executor.awaitTermination(DURATION+15000+EXTRA_WAIT_TIME, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		executor.shutdown();
		
	}
	
	/**
	 * @throws InterruptedException
	 * @throws IOException 
	 * @throws EncryptionException 
	 */
	public static void sequential_latency_test() throws InterruptedException, IOException, EncryptionException{
		guidIndex = Integer.parseInt(System.getProperty("guidIndex"));
		numGuids = Integer.parseInt(System.getProperty("numGuids"));
		
		entries = new GuidEntry[numGuids];
		for(int i=0; i<numGuids; i++){
			ObjectInputStream input = new ObjectInputStream(new FileInputStream(new File("guid"+(i*1000+guidIndex))));
			entries[i] = new GuidEntry(input);
			input.close();
		}
		
		System.out.println("Start running experiment for "+(withSignature?"signed":"unsigned")+" "+(isRead?"read":"write"));
		executor.execute(new GNSClientTask(clients[0], entries, ((Integer) RATE).doubleValue(), TOTAL));		
		while(getRcvd() < TOTAL ){
			System.out.println("Client received "+received+" responses, "+(TOTAL-received)+" left.");
			Thread.sleep(1000);
		}
		System.out.println("Received all responses!");
		executor.shutdown();
	}
	
	private static void processArgs(String[] args) throws IOException {
		Config.register(args);
	}
	
	static class GNSClientTask implements Runnable{
		private final GNSClientCommands client;
		private final GuidEntry[] entries;
		private final double rate;
		private final int total;
		
		GNSClientTask(GNSClientCommands client, GuidEntry[] entries, double rate, int total){
			this.client = client;
			this.entries = entries;
			this.rate = rate;
			this.total = total;
		}
		
		@Override
		public void run() {
			
			RateLimiter rateLimiter = new RateLimiter(rate);
			
			long t1 = System.currentTimeMillis();
			for (int i=0; i<total; i++){
				if(!executor.isShutdown()){
					executor.submit(isRead?new ReadTask(client, entries[i%numGuids], withSignature, true):new WriteTask(client, entry, withSignature, true));
					rateLimiter.record();
				}
			}
			
			System.out.println("It takes "+(System.currentTimeMillis()-t1)+"ms to send all requests.");
		}		
	}
	
	static class SingleGNSClientTask implements Runnable{
		private final GNSClientCommands client;
		private final GuidEntry entry;
		private final double rate;
		private final int total;
		private boolean needWarmUp;
		
		SingleGNSClientTask(GNSClientCommands client, GuidEntry entry, double rate, int total, boolean needWarmUp){
			this.client = client;
			this.entry = entry;
			this.rate = rate;
			this.total = total;
			this.needWarmUp = needWarmUp;
		}
		
		@Override
		public void run() {
			
			RateLimiter rateLimiter = new RateLimiter(rate);
			/**
			 * warm up for 1 round
			 */
			if(needWarmUp){
				for (int i=0; i<rate*10; i++){
					executor.submit(isRead?new ReadTask(client, entry, withSignature, false):new WriteTask(client, entry, withSignature, false));
					rateLimiter.record();
				}
			}
			
			
			/**
			 * 2nd round
			 */
			long t1 = System.currentTimeMillis();
			System.out.println("Start running with 2nd round");
			for (int i=0; i<total; i++){
				if(!executor.isShutdown()){
					executor.submit(isRead?new ReadTask(client, entry, withSignature, true):new WriteTask(client, entry, withSignature, true));
					rateLimiter.record();
				}
			}
			
			System.out.println("It takes "+(System.currentTimeMillis()-t1)+"ms to send all requests.");
		}		
	}
	
	static class ReadTask implements Runnable {
		private final GNSClientCommands client;
		private final GuidEntry guid;
		private final boolean signed;
		private final boolean log;
		
		ReadTask(GNSClientCommands client, GuidEntry guid, boolean signed, boolean log){
			this.client = client;
			this.guid = guid;
			this.signed = signed;
			this.log = log;
		}
		
		@Override
		public void run() {
			long t = System.nanoTime();
			try {
				if (signed)
					client.fieldRead(guid, someField);
				else
					client.fieldRead(guid.getGuid(),
							someField, null);
			} catch (Exception e) {
				//e.printStackTrace();
			}
			if(log)
				increaseLatency((System.nanoTime() - t)/1000);
		}
		
	}
	
	static class WriteTask implements Runnable {
		private final GNSClientCommands client;
		private final GuidEntry guid;
		private final boolean signed;
		private final boolean log;
		
		WriteTask(GNSClientCommands client, GuidEntry guid, boolean signed, boolean log){
			this.client = client;
			this.guid = guid;
			this.signed = signed;
			this.log = log;
		}
		
		@Override
		public void run() {
			long t = System.nanoTime();
			try {
				if (signed)
					client.fieldUpdate(guid.getGuid(), someField, someValue, guid);
				else
					client.fieldUpdate(guid.getGuid(),
							someField, someValue, guid);				
			} catch (Exception e) {
				e.printStackTrace();
			}
			if(log)
				increaseLatency((System.nanoTime() - t)/1000);
		}
		
	}
	
	/**
	 * @throws FileNotFoundException
	 * @throws InterruptedException 
	 */
	public static void dump() throws FileNotFoundException, InterruptedException{
		Thread.sleep(1000);
		
		resultFile = "result";
		if(System.getProperty("resultFile")!= null){
			resultFile = System.getProperty("resultFile");
		}
		
		System.out.println("Start dumping the result to file "+resultFile);
		
		PrintWriter writer = new PrintWriter(resultFile);
		/*
		for (long lat:latency){
			writer.write(lat+"\n");			
		}*/
		writer.write(received+" "+elapsed/received+"\n");
		writer.flush();
		writer.close();
		System.out.println("There are "+received+" requests, and average latency is "+Util.df(elapsed/received)+"us");
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception{
		Util.assertAssertionsEnabled();
		processArgs(args);
		
		setup();	
		if(sequential){
			long t = System.currentTimeMillis();
			sequential_latency_test();
			dump();
			System.out.println("Total time:"+(System.currentTimeMillis()-t)+"ms");
		} else {
			latency_test();
			dump();
		}
		
		System.exit(0);
	}
}
