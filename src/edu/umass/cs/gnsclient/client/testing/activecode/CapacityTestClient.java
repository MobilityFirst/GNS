package edu.umass.cs.gnsclient.client.testing.activecode;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import edu.umass.cs.gigapaxos.testing.TESTPaxosConfig.TC;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.testing.GNSClientCapacityTest;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Util;

/**
 * @author gaozy
 *
 */
public class CapacityTestClient {
	
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
	private final static long DURATION = 60*1000;
	
	private static int numClients;
	private static String someField;
	private static boolean withSigniture; 	
	private static double RATE;
	private static String resultFile;
	
	private static GuidEntry entry;
	private static GNSClientCommands[] clients;
	private static ArrayList<Long> latency = new ArrayList<Long>();
	private static ExecutorService executor;
	
	/**
	 * @throws Exception
	 */
	@BeforeClass
	public static void setup() throws Exception {
		numClients = Config.getGlobalInt(TC.NUM_CLIENTS);
		
		resultFile = "ActiveGNS/result";
		if(System.getProperty("resultFile")!= null){
			resultFile = System.getProperty("resultFile");
		}
		
		someField = "someField";
		if(System.getProperty("field")!=null){
			someField = System.getProperty("field");
		}
		
		withSigniture = false;
		if(System.getProperty("withSigniture")!= null){
			withSigniture = Boolean.parseBoolean(System.getProperty("withSigniture"));
		}
		
		RATE = 10;
		if(System.getProperty("rate")!=null){
			RATE = Long.parseLong(System.getProperty("rate"));
		}
		
		String keyFile = "ActiveGNS/guid";
		if(System.getProperty("keyFile")!= null){
			keyFile = System.getProperty("keyFile");
		}
		ObjectInputStream input = new ObjectInputStream(new FileInputStream(new File(keyFile)));
		entry = new GuidEntry(input);
		assert(entry != null);
		
		executor = Executors.newFixedThreadPool(numClients);
		
		for (int i=0; i<numClients; i++){
			clients[i] = new GNSClientCommands();
		}
	}
	
	/**
	 * 
	 */
	@Test
	public void latency_test(){
		for(int i=0; i<numClients; i++){
			executor.execute(new SingleGNSClientTask(clients[i], entry, DURATION, RATE));
		}
		executor.shutdown();
		
		try {
			executor.awaitTermination(DURATION+1, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
	
	private static void processArgs(String[] args) throws IOException {
		Config.register(args);
	}
	
	static class SingleGNSClientTask implements Runnable{
		private final GNSClientCommands client;
		private final GuidEntry entry;
		private final ExecutorService executor;
		private final long duration;
		private final double rate;
		
		SingleGNSClientTask(GNSClientCommands client, GuidEntry entry, long duration, double rate){
			this.client = client;
			this.entry = entry;			
			this.duration = duration;
			this.rate = rate;
			executor = Executors.newFixedThreadPool(10);
		}
		
		@Override
		public void run() {
			long begin = System.currentTimeMillis();
			while(System.currentTimeMillis() - begin < duration ){
				executor.submit(new ReadTask(client, entry, withSigniture));
				long delay = getInterval(rate).longValue();
				try {
					Thread.sleep(delay);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			executor.shutdown();
			try {
				executor.awaitTermination(1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
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

				latency.add(System.nanoTime() - t);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
	
	/**
	 * @throws FileNotFoundException
	 * @throws InterruptedException 
	 */
	@AfterClass
	public void cleanup() throws FileNotFoundException, InterruptedException{
		Thread.sleep(1000);
		
		System.out.println("Start dumping the result to file "+resultFile);
		
		PrintWriter writer = new PrintWriter(resultFile);
		for (long lat:latency){
			writer.write(lat+"\n");			
		}
		writer.flush();
		writer.close();
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException{
		Util.assertAssertionsEnabled();
		processArgs(args);
		Result result = JUnitCore.runClasses(GNSClientCapacityTest.class);
		for (Failure failure : result.getFailures()) {
			System.out.println(failure.getMessage());
			failure.getException().printStackTrace();
		}
	}
}
