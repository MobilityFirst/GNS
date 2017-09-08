package edu.umass.cs.gnsclient.client.testing.activecode;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.umass.cs.gigapaxos.testing.TESTPaxosConfig.TC;
import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.GNSClientConfig.GNSCC;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Util;

/**
 * @author gaozy
 *
 */
public class CapacityTestForThruputClient {
	
final static Random random = new Random();
	
	private static int NUM_THREAD = 120;
	
	private static int numClients;
	private static String someField = "someField";
	private static String someValue = "someValue";
	
	private static boolean withMalicious;
	private static boolean withSignature;
	private static boolean isRead;
	
	private static double fraction;
	
	private static GuidEntry entry;
	private static GNSClientCommands[] clients;
	private static GuidEntry malEntry;
	
	private static double ratio = 0.0;
	
	private static ExecutorService executor;
	
	
	private static int numFinishedReads = 0;
	private static long lastReadFinishedTime = System.currentTimeMillis();

	synchronized static void incrFinishedReads() {
		numFinishedReads++;
		lastReadFinishedTime = System.currentTimeMillis();
	}
	
	
	/**
	 * @throws Exception
	 */
	public static void setup() throws Exception {
		numClients = 10;
		if(System.getProperty("numClients") != null){
			numClients = Integer.parseInt(System.getProperty("numClients"));
		}
		
		someField = "someField";
		if(System.getProperty("field")!=null){
			someField = System.getProperty("field");
		}
		
		withMalicious = false;
		if(System.getProperty("withMalicious")!= null){
			withMalicious = Boolean.parseBoolean(System.getProperty("withMalicious"));
		}
		
		withSignature = false;
		if(System.getProperty("withSigniture")!= null){
			withSignature = Boolean.parseBoolean(System.getProperty("withSigniture"));
		}
				
		isRead = true;
		if(System.getProperty("isRead")!=null){
			isRead = Boolean.parseBoolean(System.getProperty("isRead"));
		}
		
		if(System.getProperty("numThread")!=null){
			NUM_THREAD = Integer.parseInt(System.getProperty("numThread"));
		}
		
		String keyFile = "guid";
		if(System.getProperty("keyFile")!= null){
			keyFile = System.getProperty("keyFile");
		}
		ObjectInputStream input = new ObjectInputStream(new FileInputStream(new File(keyFile)));
		entry = new GuidEntry(input);
		input.close();
		assert(entry != null);
		
		String malKeyFile = "mal_guid";
		if(System.getProperty("malKeyFile") != null){
			malKeyFile = System.getProperty("malKeyFile");
		}
		if(new File(malKeyFile).exists()){
			input = new ObjectInputStream(new FileInputStream(new File(malKeyFile)));
			malEntry = new GuidEntry(input);
		}
		
		fraction = 0.0;
		if(System.getProperty("fraction")!=null){
			fraction = Double.parseDouble(System.getProperty("fraction"));
		}
		
		ratio = 0.0;
		if(System.getProperty("ratio")!=null){
			ratio = Double.parseDouble(System.getProperty("ratio")); 
		}
		
		executor = Executors.newFixedThreadPool(NUM_THREAD);
		
		clients = new GNSClientCommands[numClients];
		for (int i=0; i<numClients; i++){
			clients[i] = new GNSClientCommands(new GNSClient());
		}
	}
	
	private static void blockingRead(int clientIndex, GuidEntry guid, boolean signed) {
		executor.submit(new Runnable() {
			public void run() {
				try {
					if (signed)
						clients[clientIndex].fieldRead(guid, someField);
					else
						clients[clientIndex].fieldRead(guid.getGuid(),
								someField, null);					
				} catch (Exception e) {
					//e.printStackTrace();
				}
				incrFinishedReads();
			}
		});
	}
	

	
	private static void blockingWrite(int clientIndex, GuidEntry guid, boolean signed) {
		executor.submit(new Runnable() {
			public void run() {
				try {
					if (signed)
						clients[clientIndex].fieldUpdate(guid, someField, someValue);
					else
						clients[clientIndex].fieldUpdate(guid, someField, someValue);					
				} catch (Exception e) {
					//e.printStackTrace();
				}
				incrFinishedReads();
			}
		});
	}
	
	
	/**
	 * @throws InterruptedException
	 */
	public static void thru_test() throws InterruptedException{
		
		String operation = (isRead?"read":"write");
		String signed = (withSignature?"signed":"unsigned");
		
		int numReads = Math.min(
				Config.getGlobalBoolean(GNSCC.ENABLE_SECRET_KEY) ? Integer.MAX_VALUE : 10000,
				Config.getGlobalInt(TC.NUM_REQUESTS));
		long t = System.currentTimeMillis();
		for (int i = 0; i < numReads; i++) {
			if(isRead){
				blockingRead(i % numClients, entry, withSignature);
			}else{
				blockingWrite(i % numClients, entry, withSignature);
			}
		}
		System.out.print("[total_"+signed+"_"+operation+"=" + numReads+": ");
		int lastCount = 0;
		while (numFinishedReads < numReads) {
			if(numFinishedReads>lastCount)  {
				lastCount = numFinishedReads;
				System.out.print(numFinishedReads + "@" + Util.df(numFinishedReads * 1.0 / (lastReadFinishedTime - t))+"K/s ");
			}
			Thread.sleep(1000);
		}
		System.out.println("] ");
		
		
		System.out.println("parallel_"+signed+"_"+operation+"_rate="
				+ Util.df(numReads * 1.0 / (lastReadFinishedTime - t))
				+ "K/s");
	}
	
	/**
	 * @throws InterruptedException 
	 * 
	 */
	public static void sequential_thru_test() throws InterruptedException{
		String signed = withSignature?"signed":"unsigned";
		String operation = isRead?"read":"write";		
		
		assert(malEntry != null):"Malicious guid can not be null";
		assert(ratio > 0.0):"ration can't be 0 or negative";
		int numReads = Config.getGlobalInt(TC.NUM_REQUESTS);
		int malReads = ((Number) (numReads*fraction/(fraction+ratio*(1-fraction)))).intValue();
				
		System.out.println("Start running experiment with "+numReads+" benign requests and "+malReads+" malicious requests, "
				+ "the ratio is "+ratio+" with the malicious fraction="+fraction);
		long t = System.currentTimeMillis();
		
		new Thread(){
			public void run(){
				for (int i = 0; i < numReads-malReads; i++) {
					blockingRead(i % numClients, entry, withSignature);
				}
			}
		}.start();
		new Thread(){
			public void run(){
				for (int i = 0; i < malReads; i++) {
					blockingRead(i % numClients, malEntry, withSignature);
				}
			}
		}.start();
		
		System.out.print("[total_"+signed+"_"+operation+"=" + numReads+": ");
		int lastCount = 0;
		while (numFinishedReads < numReads) {
			if(numFinishedReads>lastCount)  {
				lastCount = numFinishedReads;
				System.out.print(numFinishedReads + "@" + Util.df(numFinishedReads * 1.0 / (lastReadFinishedTime - t))+"K/s ");
			}
			Thread.sleep(1000);
		}
		System.out.println("] ");
		
		System.out.println("parallel_"+signed+"_"+operation+"_rate="
				+ Util.df(numReads * 1000.0 / (lastReadFinishedTime - t))
				+ "/s");
	}
	
	
	private static void processArgs(String[] args) throws IOException {
		Config.register(args);
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception{
		Util.assertAssertionsEnabled();
		processArgs(args);
		
		setup();
		
		if(!withMalicious)
			thru_test();
		else
			sequential_thru_test();
				
		System.exit(0);
	}
}
