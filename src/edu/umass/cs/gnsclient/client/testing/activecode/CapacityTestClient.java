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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import edu.umass.cs.gigapaxos.testing.TESTPaxosConfig.TC;
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
		
		String keyFile = "guid";
		if(System.getProperty("keyFile")!= null){
			keyFile = System.getProperty("keyFile");
		}
		ObjectInputStream input = new ObjectInputStream(new FileInputStream(new File(keyFile)));
		entry = new GuidEntry(input);
		assert(entry != null);
		
		executor = Executors.newFixedThreadPool(numClients);
		
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
	@Test
	public void latency_test() throws FileNotFoundException, InterruptedException{
		for(int i=0; i<numClients; i++){
			executor.execute(new SingleGNSClientTask(clients[i], entry, DURATION, RATE));
		}
		executor.shutdown();
		
		try {
			executor.awaitTermination(DURATION+5000, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		cleanup();
	}
	
	private static void processArgs(String[] args) throws IOException {
		Config.register(args);
	}
	
	static class SingleGNSClientTask implements Runnable{
		private final GNSClientCommands client;
		private final GuidEntry entry;
		private final ScheduledExecutorService executor ;
		private final long duration;
		private final double rate;
		
		SingleGNSClientTask(GNSClientCommands client, GuidEntry entry, long duration, double rate){
			this.client = client;
			this.entry = entry;			
			this.duration = duration;
			this.rate = rate;
			executor = Executors.newScheduledThreadPool(10);
		}
		
		@Override
		public void run() {
			executor.scheduleAtFixedRate(new ReadTask(client, entry, withSigniture), 0, ((Double) (1000.0/rate)).longValue(), TimeUnit.MILLISECONDS);
			
			executor.shutdown();
			try {
				executor.awaitTermination(duration+1000, TimeUnit.MILLISECONDS);
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
			long t = System.currentTimeMillis();
			try {
				if (signed)
					client.fieldRead(guid, someField);
				else
					client.fieldRead(guid.getGuid(),
							someField, null);

				latency.add(System.currentTimeMillis() - t);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
	
	/**
	 * @throws FileNotFoundException
	 * @throws InterruptedException 
	 */
	public static void cleanup() throws FileNotFoundException, InterruptedException{
		Thread.sleep(1000);
		
		resultFile = "result";
		if(System.getProperty("resultFile")!= null){
			resultFile = System.getProperty("resultFile");
		}
		
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
		Result result = JUnitCore.runClasses(CapacityTestClient.class);
		for (Failure failure : result.getFailures()) {
			System.out.println(failure.getMessage());
			failure.getException().printStackTrace();
		}
	}
}
