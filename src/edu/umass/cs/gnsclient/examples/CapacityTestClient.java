package edu.umass.cs.gnsclient.examples;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.UniversalTcpClient;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.exceptions.GnsException;

public class CapacityTestClient {
	private final static String key_folder = "/Users/zhaoyugao/gns_key/";
	private static String ACCOUNT_ALIAS = "@cs.umass.edu";
	
	private static ArrayList<Long> latency = new ArrayList<Long>();
    private ThreadPoolExecutor executorPool;
    private static int NUM_THREAD = 10;
    private static long start = 0;
    protected static int TOTAL_SECOND = 1;
    private static final int NUM_CLIENT = 10;
    
    
    private UniversalTcpClient client;
    private String guid;
    private GuidEntry entry;
            
    protected CapacityTestClient(UniversalTcpClient client, String guid, GuidEntry entry){
    	this.client = client;
    	this.guid = guid;
    	this.entry = entry;
    	
    	executorPool = new ThreadPoolExecutor(NUM_THREAD, NUM_THREAD, 0, TimeUnit.SECONDS, 
	    		new LinkedBlockingQueue<Runnable>(), new MyThreadFactory() );
    	executorPool.prestartAllCoreThreads();
    }
    
    protected synchronized static void updateLatency(long time){
    	CapacityTestClient.latency.add(time);
    }
     
    protected synchronized static int num_received(){	
    	return latency.size();
    }    
    
    private void sendSingleRequest(){
    	this.executorPool.execute(new ClientThread(this.client, this.guid, this.entry));
    	//(new Thread(new ClientThread())).start();
    }
    
    private static void sendRequests(int numRequest, int rate, CapacityTestClient[] clients){
    	int reqPerClient = numRequest / NUM_CLIENT;
    	RateLimiter r = new RateLimiter(rate);
    	System.out.println("Start sending rate at "+rate+" req/sec with "+ NUM_CLIENT +" clients...");
    	for (int i=0; i<reqPerClient; i++){
    		for(int j=0; j<NUM_CLIENT; j++){
    			clients[j].sendSingleRequest();
    			r.record();
    		}
    	}
    }
	
    private class ClientThread implements Runnable{
    	private UniversalTcpClient client;
        private String guid;
        private GuidEntry entry;
        
    	public ClientThread(UniversalTcpClient client, String guid, GuidEntry entry){
    		this.client = client;
    		this.guid = guid;
    		this.entry = entry;
    	}
    	
    	public void run(){
    		long begin = System.currentTimeMillis();
    		try{
    			client.fieldRead(guid, "hi", entry);
    		}catch(Exception e){
    			e.printStackTrace();
    		}
    		
    		long elapsed = System.currentTimeMillis() - begin;
    		CapacityTestClient.updateLatency(elapsed);
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
		int rate =  Integer.parseInt(args[0]);
		int node = Integer.parseInt(args[1]);
		
		CapacityTestClient[] clients = new CapacityTestClient[NUM_CLIENT];
		
		for (int index=0; index<10; index++){
			UniversalTcpClient client = new UniversalTcpClient("0.0.0.0", 24398);
			String account = "test"+(node*10+index)+ACCOUNT_ALIAS;
			System.out.println("The account is "+account);
		
			//String guid = client.lookupGuid(account);
			GuidEntry accountGuid = KeyPairUtils.getGuidEntry(client.getGnsRemoteHost() + ":" + client.getGnsRemotePort(), account);
			String guid = accountGuid.getGuid();
		
			System.out.println("The GUID is "+guid);
			
			clients[index] = new CapacityTestClient(client, guid, accountGuid);
		}
		
		int TOTAL = rate * 30;
		
		start = System.currentTimeMillis();
    	long t1 = System.currentTimeMillis();
    	sendRequests(TOTAL, rate, clients);
    	long t2 = System.currentTimeMillis();
    	long elapsed = t2 - t1;
    	
    	System.out.println("It takes "+elapsed+"ms.");
    	
    	System.out.println("There are "+TOTAL+" requests.");
    	while(latency.size() != TOTAL){
    		
    		System.out.println("Received "+latency.size()+" messages totally");
    		try{
    			Thread.sleep(1000);
    		}catch(Exception e){
    			e.printStackTrace();
    		}
    		
    	}

    	long total = 0;
    	for(long t:latency){
    		total += t;
    	}
    	System.out.println("The average latency is "+total/latency.size()+"ms");
    	System.out.println("The start point is:"+(start/1000));
    	System.exit(0);		
	}
}
