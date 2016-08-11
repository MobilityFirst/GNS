package edu.umass.cs.gnsclient.client.testing.activecode;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.FixMethodOrder;

import edu.umass.cs.gigapaxos.paxosutil.RateLimiter;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DefaultTest;
import edu.umass.cs.utils.Util;

/**
 * @author gaozy
 *
 */
@FixMethodOrder(org.junit.runners.MethodSorters.NAME_ASCENDING)
public class CapacityTestClient extends DefaultTest {
	
	final static Random random = new Random();
	
	/**
	 * Generate a random interval following Poisson process
	 * @param rate
	 * @return
	 */
	private static Double getInterval(double rate){
		return -Math.log(1-random.nextDouble())/ rate;
	}
	
	/**
	 * Test for 1 min
	 */
	private final static int DURATION = 60*1000;
	
	private static int numClients;
	private static String someField;
	private static boolean withSignature; 	
	private static int RATE;
	private static String resultFile;
	private static int TOTAL;
	
	private static GuidEntry entry;
	private static GNSClientCommands[] clients;
	private static List<Long> latency = new CopyOnWriteArrayList<Long>();
	
	private static ExecutorService executor;
	
	private static int total = 0;
	private static long elapsed = 0;
	static synchronized void increaseLatency(long lat){
		total++;
		elapsed += lat;
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
		TOTAL = RATE*DURATION;
		
		String keyFile = "guid";
		if(System.getProperty("keyFile")!= null){
			keyFile = System.getProperty("keyFile");
		}
		ObjectInputStream input = new ObjectInputStream(new FileInputStream(new File(keyFile)));
		entry = new GuidEntry(input);
		assert(entry != null);
		
		executor = Executors.newFixedThreadPool(numClients*10);
		
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
		for(int i=0; i<numClients; i++){
			executor.execute(new SingleGNSClientTask(clients[i], entry, ((Integer) RATE).doubleValue(), TOTAL));
		}
		
		try {
			executor.awaitTermination(DURATION+1000, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		executor.shutdown();
	}
	
	private static void processArgs(String[] args) throws IOException {
		Config.register(args);
	}
	
	static class SingleGNSClientTask implements Runnable{
		private final GNSClientCommands client;
		private final GuidEntry entry;
		private final double rate;
		private final int total;
		
		SingleGNSClientTask(GNSClientCommands client, GuidEntry entry, double rate, int total){
			this.client = client;
			this.entry = entry;
			this.rate = rate;
			this.total = total;
		}
		
		@Override
		public void run() {
			RateLimiter rateLimiter = new RateLimiter(rate);
			/**
			 * Run two rounds for experiment, the first round is for warming up
			 */
			System.out.println("round 1");
			for (int i=0; i<total; i++){
				executor.submit(new ReadTask(client, entry, withSignature));
				rateLimiter.record();
			}
		}		
	}
	
	static class ReadTask implements Runnable {
		private final GNSClientCommands client;
		private final GuidEntry guid;
		private final boolean signed;
		
		ReadTask(GNSClientCommands client, GuidEntry guid, boolean signed){
			this.client = client;
			this.guid = guid;
			this.signed = signed;
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

				//latency.add(System.currentTimeMillis() - t);
				increaseLatency((System.nanoTime() - t)/1000);
			} catch (Exception e) {
				e.printStackTrace();
			}
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
		writer.write(total+" "+elapsed/total+"\n");
		writer.flush();
		writer.close();
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception{
		Util.assertAssertionsEnabled();
		processArgs(args);
		
		setup();
		latency_test();
		dump();
		
		System.exit(0);
	}
}
