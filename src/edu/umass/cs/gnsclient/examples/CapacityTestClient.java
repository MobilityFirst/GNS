package edu.umass.cs.gnsclient.examples;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.UniversalTcpClient;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.exceptions.GnsException;

public class CapacityTestClient {
	//private final static String key_folder = "/Users/zhaoyugao/gns_key/";
	private final static String ACCOUNT_ALIAS = "@gigapaxos.net";
	//private final static String EC2_ADDRESS = "52.88.106.121";
	private static ArrayList<Long> latency = new ArrayList<Long>();
	private static ArrayList<Integer> mal_request = new ArrayList<Integer>();
	private static int req_id = 0;
    private static ThreadPoolExecutor executorPool;
    //private static ThreadPoolExecutor executorMal; // This is for executing malicious request
    
    private static int NUM_THREAD = 100;
    private static long start = 0;
    private static int NUM_CLIENT = 0;
    private static int INTERVAL = 1;
    private static final int DURATION = 60;
    private final static int MALICIOUS_EVERY_FEW_CLIENTS = 5;
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
    	failed = 0;
    }
    
    protected synchronized static int num_received(){	
    	return latency.size();
    }    
    
    private void sendSingleRequest(UniversalTcpClient client, boolean malicious){
    	executorPool.execute(new ClientThread(client, this.guid, this.entry, malicious));
    	//(new Thread(new ClientThread())).start();
    }
    
    /*
    private void sendMaliciousRequest(UniversalTcpClient client, boolean malicious){
    	executorPool.execute(new MaliciousThread(client, this.guid, this.entry));
    	
    	Future<String> future = executor.submit(new MaliciousThread(client, this.guid, this.entry));
    	try {
    		future.get();
    	}catch(InterruptedException e){
       		e.printStackTrace();
    	}catch(ExecutionException e){
    		e.printStackTrace();
    	}catch(Exception e){
    		e.printStackTrace();
    	}
    	
    }
	*/
    
    private static void sendRequests(int numRequest, int rate, CapacityTestClient[] clients, int fraction, UniversalTcpClient client){
    	int reqPerClient = numRequest / NUM_CLIENT;
    	RateLimiter r = new RateLimiter(rate);
    	System.out.println("Start sending rate at "+rate+" req/sec with "+ NUM_CLIENT +" guid...");
    	for (int i=0; i<reqPerClient; i++){
    		for(int j=0; j<NUM_CLIENT; j++){
    			if(j%MALICIOUS_EVERY_FEW_CLIENTS < fraction){
    				clients[j].sendSingleRequest(client, false);
    			}else{
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
    			client.fieldRead(guid, "hi", entry);
    			//System.out.println("The response is "+result);
    		}catch(GnsException e){
    			failed++;
    		}catch(Exception e){
    			e.printStackTrace();
    		}
    		
    		long elapsed = System.nanoTime() - begin;
    		if(!this.mal){
    			System.out.println(elapsed);
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
     * @throws GnsException
     * @throws InvalidKeyException
     * @throws SignatureException
     * @throws Exception
     */
	public static void main(String[] args) throws IOException,
    InvalidKeySpecException, NoSuchAlgorithmException, GnsException,
    InvalidKeyException, SignatureException, Exception {
		String address = args[0];
		NUM_CLIENT =  Integer.parseInt(args[1]);
		INTERVAL = Integer.parseInt(args[2]);
		int rate = INTERVAL*NUM_CLIENT;
		int node = Integer.parseInt(args[3]); 
		int fraction = Integer.parseInt(args[4]);
		if(fraction > MALICIOUS_EVERY_FEW_CLIENTS ){
			System.out.println("The fraction of malicious users must lie between 0 to 5 (0%~100%).");
			System.exit(0);
		}
		fraction = MALICIOUS_EVERY_FEW_CLIENTS - fraction;
		
		CapacityTestClient[] clients = new CapacityTestClient[NUM_CLIENT];
		UniversalTcpClient client = new UniversalTcpClient(address, 24398, true);
		
    	executorPool = new ThreadPoolExecutor(NUM_THREAD, NUM_THREAD, 0, TimeUnit.SECONDS, 
	    		new LinkedBlockingQueue<Runnable>(), new MyThreadFactory() );
    	executorPool.prestartAllCoreThreads();
    	/*
    	executorMal = new ThreadPoolExecutor(NUM_THREAD, NUM_THREAD, 0, TimeUnit.SECONDS, 
	    		new LinkedBlockingQueue<Runnable>(), new MyThreadFactory() );
    	executorMal.prestartAllCoreThreads();
    	*/
    	//Executors.newFixedThreadPool(NUM_THREAD);//Executors.newSingleThreadExecutor();
		
    	
		for (int index=0; index<NUM_CLIENT; index++){			
			String account = "test"+(node*1000+index)+ACCOUNT_ALIAS;
			System.out.println("The account is "+account);
		
			//String guid = client.lookupGuid(account);
			
			GuidEntry accountGuid = KeyPairUtils.getGuidEntry(address + ":" + client.getGnsRemotePort(), account);
			String guid = accountGuid.getGuid();
		
			System.out.println("The GUID is "+guid);
			
			clients[index] = new CapacityTestClient(guid, accountGuid);
		}
		
		int TOTAL = rate * DURATION;
		
		System.out.println("1st run");
		start = System.currentTimeMillis();
    	long t1 = System.currentTimeMillis();
    	sendRequests(TOTAL, rate, clients, fraction, client);
    	long t2 = System.currentTimeMillis();
    	long elapsed = t2 - t1;
    	
    	System.out.println("It takes "+elapsed+"ms.");
    	
    	int TOTAL_NORMAL = TOTAL * fraction / MALICIOUS_EVERY_FEW_CLIENTS;
    	
    	System.out.println("There are "+TOTAL+" requests, and "+TOTAL_NORMAL+" normal requests.");
    	while((latency.size()+failed) < TOTAL_NORMAL){
    		
    		System.out.println("Received "+(latency.size()+failed)+" messages totally");
    		try{
    			Thread.sleep(1000);
    		}catch(Exception e){
    			e.printStackTrace();
    		}
    		
    	}
    	Collections.sort(latency);
    	System.out.println("There are "+failed+" requests failed.");
    	System.out.println("The median latency is "+latency.get(latency.size()/2)/1000000.0+"ms");
    	System.out.println("The start point is:"+(start/1000));
    	while(mal_request.size() != (TOTAL - TOTAL_NORMAL)){
    		System.out.println("Finished malicious requests "+mal_request.size());
    		try{
    			Thread.sleep(1000);
    		}catch(InterruptedException e){
    			e.printStackTrace();
    		}
    	}
    	
    	/*
    	System.out.println("2nd run");
    	start = System.currentTimeMillis();
    	t1 = System.currentTimeMillis();
    	sendRequests(TOTAL, rate, clients, fraction, client);
    	t2 = System.currentTimeMillis();
    	elapsed = t2 - t1;
    	
    	System.out.println("It takes "+elapsed+"ms.");
    	
    	TOTAL_NORMAL = TOTAL * fraction / MALICIOUS_EVERY_FEW_CLIENTS;
    	
    	System.out.println("There are "+TOTAL+" requests, and "+TOTAL_NORMAL+" normal requests.");
    	while((latency.size()+failed) < TOTAL_NORMAL){
    		
    		System.out.println("Received "+(latency.size()+failed)+" messages totally");
    		try{
    			Thread.sleep(1000);
    		}catch(Exception e){
    			e.printStackTrace();
    		}
    		
    	}

    	Collections.sort(latency);
    	System.out.println("There are "+failed+" requests failed.");
    	System.out.println("The median latency is "+latency.get(latency.size()/2)/1000000.0+"ms");
    	System.out.println("The start point is:"+(start/1000));
    	*/ 
    	
    	// connect to none server and inform it's done
    	Socket socket = new Socket("128.119.245.5", 60001);
    	PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
    	out.println(node+"\n");
    	socket.close();
    	
    	System.exit(0);		
	}
}
