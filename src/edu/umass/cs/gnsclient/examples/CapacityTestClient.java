package edu.umass.cs.gnsclient.examples;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.UniversalTcpClient;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;

/**
 * @author gaozy
 *
 */
public class CapacityTestClient {
	private final static String ACCOUNT_ALIAS = "@gigapaxos.net";
	private final static int NUM_CLIENT = 5;
	
	private static ArrayList<Long> latency = new ArrayList<Long>();
	private static ArrayList<Integer> mal_request = new ArrayList<Integer>();
	private static int req_id = 0;
    private static ThreadPoolExecutor executorPool;
        
    private static int NUM_THREAD = 100;    
    private static int NUM_GUID = 1;
    private static int BENIGN_GUID = 0;
    private static int INTERVAL = 1;
    private static final int DURATION = 10;

    private static int failed = 0;
    
    
    protected synchronized static void updateLatency(long time){
    	CapacityTestClient.latency.add(time);
    }
     
    protected synchronized static void clearLatency(){
    	CapacityTestClient.latency.clear();
    	CapacityTestClient.mal_request.clear();
    	failed = 0;
    }
    
    protected synchronized static int num_received(){	
    	return latency.size();
    }    
    
    private static void sendSingleRequest(GNSClient client, GuidEntry entry, boolean malicious){
    	executorPool.execute(new ClientThread(client, entry, malicious));
    }

    
    private static void sendRequests(int numRequest, int rate, GuidEntry[] guidEntries, GNSClient[] clients){
    	int reqPerClient = numRequest / NUM_CLIENT;
    	RateLimiter r = new RateLimiter(rate);
    	System.out.println("Start sending rate at "+rate+" req/sec with "+ NUM_CLIENT +" clients...");
    	int k = 0;
    	int g = 0;
    	for (int i=0; i<reqPerClient; i++){
    		for(int j=0; j<NUM_CLIENT; j++){
    			if(j < BENIGN_GUID ){
    				//send benign request
    				sendSingleRequest(clients[k], guidEntries[g], false);
    			}else{
    				//send malicious request
    				sendSingleRequest(clients[k], guidEntries[g], true);
    			}
    			r.record();
    			k = (k+1)%NUM_CLIENT;
    			g = (g+1)%NUM_GUID;
    		}
    	}
    }
	
    private static class ClientThread implements Runnable{
    	private GNSClient client;
        private GuidEntry entry;
        private boolean mal;
        
    	public ClientThread(GNSClient client, GuidEntry entry, boolean mal){
    		this.client = client;
    		this.entry = entry;
    		this.mal = mal;
    	}
    	
    	public synchronized void run(){
    		long begin = System.currentTimeMillis();
    		try{
    			client.fieldRead(entry.getGuid(), "nextGuid", entry);
    		}catch(Exception e){
    			failed++;
    			e.printStackTrace();
    		}
    		
    		long elapsed = System.currentTimeMillis() - begin;
    		if(!this.mal){
    			CapacityTestClient.updateLatency(elapsed);
    		}else{
    			mal_request.add(req_id);
            	req_id++;
    		}
    		
    	}
    }
    
    
    /**
     * Test with a single {@link UniversalTcpClient}
     * @param args
     * @throws IOException
     * @throws InvalidKeySpecException
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     * @throws SignatureException
     * @throws Exception
     */
	public static void main(String[] args) throws IOException,
    InvalidKeySpecException, NoSuchAlgorithmException,
    InvalidKeyException, SignatureException, Exception {
		String address = args[0];
		int node = Integer.parseInt(args[1]); 
		NUM_GUID =  Integer.parseInt(args[2]);
		INTERVAL = Integer.parseInt(args[3]);
		int rate = INTERVAL*NUM_GUID;
		if(args.length == 5){
			BENIGN_GUID = Integer.parseInt(args[4]);
		} else{
			BENIGN_GUID = NUM_GUID;
		}
		ReconfigurationConfig.setConsoleHandler(Level.WARNING);
		
		GNSClient[] clients = new GNSClient[NUM_CLIENT];
		GuidEntry[] guidEntries = new GuidEntry[NUM_GUID];
		
		for (int i=0; i<NUM_CLIENT; i++){
			clients[i] = new GNSClient(null, new InetSocketAddress(address, GNSClientConfig.LNS_PORT), true);
		}
		//UniversalTcpClient cl = new UniversalTcpClient(address, 24398, false);
		//cl.guidBatchCreate(accountGuid, aliases, createPublicKeys)
		
		
    	executorPool = new ThreadPoolExecutor(NUM_THREAD, NUM_THREAD, 0, TimeUnit.SECONDS, 
	    		new LinkedBlockingQueue<Runnable>(), new MyThreadFactory() );
    	executorPool.prestartAllCoreThreads();
    	    	
		for (int i=0; i<NUM_GUID; i++){			
			String account = "test"+(node*1000+i)+ACCOUNT_ALIAS;
			System.out.println("The account is "+account);
			
			guidEntries[i] = KeyPairUtils.getGuidEntry(SequentialRequestClient.getDefaultGNSInstance(), account);
			//String guid = accountGuid.getGuid();			
		}
		
		int TOTAL = rate * DURATION;
		
		System.out.println("1st run");
    	long t1 = System.currentTimeMillis();
    	sendRequests(TOTAL, rate, guidEntries, clients);
    	long t2 = System.currentTimeMillis();
    	long elapsed = t2 - t1;
    	
    	System.out.println("It takes "+elapsed+"ms.");
    	
    	int TOTAL_NORMAL = TOTAL *  BENIGN_GUID / NUM_GUID;	
    	System.out.println("There are "+TOTAL+" requests, and "+TOTAL_NORMAL+" normal requests.");

    	while((latency.size()+failed) < TOTAL_NORMAL ){
    		
    		System.out.println("Received "+(latency.size()+failed)+" messages totally");
    		try{
    			Thread.sleep(1000);
    		}catch(Exception e){
    			e.printStackTrace();
    		}
    	}
    	
    	System.out.println("The percentage of the responsed requests is "+latency.size()/(new Double(TOTAL_NORMAL)));
    	
    	while(mal_request.size() < (TOTAL - TOTAL_NORMAL)  ){
    		System.out.println("Finished malicious requests "+mal_request.size());
    		try{
    			Thread.sleep(1000);
    		}catch(InterruptedException e){
    			e.printStackTrace();
    		}
    	}
    	
    	long total = 0;
    	for (long lat:latency){
    		total += lat;
    	}
    	System.out.println("There are "+failed+" requests failed.");
    	System.out.println("The average latency is "+(total/latency.size())+" ms");
    	
    	clearLatency();    	
    	
    	System.out.println("2nd run");
    	t1 = System.currentTimeMillis();
    	sendRequests(TOTAL, rate, guidEntries, clients);
    	t2 = System.currentTimeMillis();
    	elapsed = t2 - t1;
    	
    	System.out.println("It takes "+elapsed+"ms.");
    	
    	TOTAL_NORMAL = TOTAL * BENIGN_GUID / NUM_GUID;    	
    	System.out.println("There are "+TOTAL+" requests, and "+TOTAL_NORMAL+" normal requests.");
    	
    	while((latency.size()+failed) < TOTAL_NORMAL ){
    		
    		System.out.println("Received "+(latency.size()+failed)+" messages totally");
    		try{
    			Thread.sleep(1000);
    		}catch(Exception e){
    			e.printStackTrace();
    		}
    	}
    	
    	System.out.println("The percentage of the responsed requests is "+latency.size()/(new Double(TOTAL_NORMAL)));
    	
    	while(mal_request.size() < (TOTAL - TOTAL_NORMAL) ){
    		System.out.println("Finished malicious requests "+mal_request.size());
    		try{
    			Thread.sleep(1000);
    		}catch(InterruptedException e){
    			e.printStackTrace();
    		}
    	}
    	
    	total = 0;
    	for (long lat:latency){
    		total += lat;
    	}
    	System.out.println("There are "+failed+" requests failed.");
    	System.out.println("The average latency is "+(total/latency.size())+" ms");
    	
    	
    	/*
    	// connect to none server and inform it's done
    	Socket socket = new Socket("128.119.245.5", 60001);
    	PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
    	out.println(node+"\n");
    	socket.close();
    	*/
    	System.exit(0);		
	}
}
