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

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.UniversalTcpClient;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;

/**
 * @author gaozy
 *
 */
public class CapacityTestClient {
	private final static String ACCOUNT_ALIAS = "@gigapaxos.net";
	private static ArrayList<Long> latency = new ArrayList<Long>();
	private static ArrayList<Integer> mal_request = new ArrayList<Integer>();
	private static int req_id = 0;
    private static ThreadPoolExecutor executorPool;
        
    private static int NUM_THREAD = 100;
    private static int NUM_CLIENT = 0;
    private static int BENIGN_CLIENT = 0;
    private static int INTERVAL = 1;
    private static final int DURATION = 10;

    private static int failed = 0;
    
    protected String guid;
    protected GuidEntry entry;
            
    protected CapacityTestClient(String guid, GuidEntry entry){
    	this.guid = guid;
    	this.entry = entry;
    }
    
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
    
    private void sendSingleRequest(UniversalTcpClient client, boolean malicious){
    	executorPool.execute(new ClientThread(client, this.guid, this.entry, malicious));
    }

    
    private static void sendRequests(int numRequest, int rate, CapacityTestClient[] clients, UniversalTcpClient client){
    	int reqPerClient = numRequest / NUM_CLIENT;
    	RateLimiter r = new RateLimiter(rate);
    	System.out.println("Start sending rate at "+rate+" req/sec with "+ NUM_CLIENT +" guid...");
    	for (int i=0; i<reqPerClient; i++){
    		for(int j=0; j<NUM_CLIENT; j++){
    			if(j < BENIGN_CLIENT ){
    				//send benign request
    				clients[j].sendSingleRequest(client, false);
    			}else{
    				//send malicious request
    				clients[j].sendSingleRequest(client, true);
    			}
    			r.record();
    		}
    	}
    }
	
    private class ClientThread implements Runnable{
    	private UniversalTcpClient client;
        private String guid;
        private GuidEntry entry;
        private boolean mal;
        
    	public ClientThread(UniversalTcpClient client, String guid, GuidEntry entry, boolean mal){
    		this.client = client;
    		this.guid = guid;
    		this.entry = entry;
    		this.mal = mal;
    	}
    	
    	public synchronized void run(){
    		long begin = System.nanoTime();
    		try{
    			client.fieldRead(guid, "nextGuid", entry);
    		}catch(Exception e){
    			failed++;
    			e.printStackTrace();
    		}
    		
    		long elapsed = System.nanoTime() - begin;
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
		NUM_CLIENT =  Integer.parseInt(args[2]);
		INTERVAL = Integer.parseInt(args[3]);
		int rate = INTERVAL*NUM_CLIENT;		
		BENIGN_CLIENT = Integer.parseInt(args[4]);
		
		CapacityTestClient[] clients = new CapacityTestClient[NUM_CLIENT];
		GNSClient client = new GNSClient(null, new InetSocketAddress(address, GNSClientConfig.LNS_PORT), true);
		
		//UniversalTcpClient client = new UniversalTcpClient(address, 24398, false);
		
    	executorPool = new ThreadPoolExecutor(NUM_THREAD, NUM_THREAD, 0, TimeUnit.SECONDS, 
	    		new LinkedBlockingQueue<Runnable>(), new MyThreadFactory() );
    	executorPool.prestartAllCoreThreads();
    	    	
		for (int index=0; index<NUM_CLIENT; index++){			
			String account = "test"+(node*1000+index)+ACCOUNT_ALIAS;
			System.out.println("The account is "+account);
			
			GuidEntry accountGuid = KeyPairUtils.getGuidEntry(SequentialRequestClient.getDefaultGNSInstance(), account);
			String guid = accountGuid.getGuid();
		
			System.out.println("The GUID is "+guid);
			
			clients[index] = new CapacityTestClient(guid, accountGuid);
		}
		
		int TOTAL = rate * DURATION;
		
		System.out.println("1st run");
    	long t1 = System.currentTimeMillis();
    	sendRequests(TOTAL, rate, clients, client);
    	long t2 = System.currentTimeMillis();
    	long elapsed = t2 - t1;
    	
    	System.out.println("It takes "+elapsed+"ms.");
    	
    	int TOTAL_NORMAL = TOTAL * (NUM_CLIENT - BENIGN_CLIENT) / NUM_CLIENT;	
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
    	System.out.println("The average latency is "+(total/latency.size()));
    	
    	clearLatency();    	
    	
    	System.out.println("2nd run");
    	t1 = System.currentTimeMillis();
    	sendRequests(TOTAL, rate, clients, client);
    	t2 = System.currentTimeMillis();
    	elapsed = t2 - t1;
    	
    	System.out.println("It takes "+elapsed+"ms.");
    	
    	TOTAL_NORMAL = TOTAL * (NUM_CLIENT - BENIGN_CLIENT) / NUM_CLIENT;    	
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
    	System.out.println("The average latency is "+(total/latency.size()));
    	
    	
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
