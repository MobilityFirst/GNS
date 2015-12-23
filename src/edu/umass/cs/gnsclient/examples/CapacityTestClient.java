package edu.umass.cs.gnsclient.examples;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.UniversalTcpClient;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.exceptions.GnsException;

public class CapacityTestClient {
	//private final static String key_folder = "/Users/zhaoyugao/gns_key/";
	private final static String ACCOUNT_ALIAS = "@example.com";
	private final static String EC2_ADDRESS = "172.31.40.139";
	private static ArrayList<Long> latency = new ArrayList<Long>();
    private ThreadPoolExecutor executorPool;
    private ExecutorService executor; // This is for executing malicious request
    
    private static int NUM_THREAD = 10;
    private static long start = 0;
    protected static int TOTAL_SECOND = 1;
    private static final int NUM_CLIENT = 10;
    
    private static int failed = 0;
    
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
    	executor = Executors.newFixedThreadPool(NUM_THREAD);//Executors.newSingleThreadExecutor();
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
    
    private void sendMaliciousRequest(){
    	//(new Thread(new MaliciousThread(this.client, this.guid, this.entry)) ).start();
    	Future<String> future = this.executor.submit(new MaliciousThread(this.client, this.guid, this.entry));
    	try {
    		future.get();
    	}catch(InterruptedException e){
       		e.printStackTrace();
    	}catch(ExecutionException e){
    		e.printStackTrace();
    	}
    }
    
    private static void sendRequests(int numRequest, int rate, CapacityTestClient[] clients, int fraction){
    	int reqPerClient = numRequest / NUM_CLIENT;
    	RateLimiter r = new RateLimiter(rate);
    	System.out.println("Start sending rate at "+rate+" req/sec with "+ NUM_CLIENT +" clients...");
    	for (int i=0; i<reqPerClient; i++){
    		for(int j=0; j<NUM_CLIENT; j++){
    			if(j < fraction){
    				clients[j].sendSingleRequest();
    			}else{
    				try{
    					clients[j].sendMaliciousRequest();
    				}catch(Exception e){
    					System.out.println("Exception handled!");
    				}
    			}
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
    			//System.out.println("The response is "+result);
    		}catch(GnsException e){
    			failed++;
    		}catch(Exception e){
    			e.printStackTrace();
    		}
    		
    		long elapsed = System.currentTimeMillis() - begin;
    		CapacityTestClient.updateLatency(elapsed);
    	}
    }
    
    private class MaliciousThread implements Callable<String>{
    	private UniversalTcpClient client;
        private String guid;
        private GuidEntry entry;
        
    	public MaliciousThread(UniversalTcpClient client, String guid, GuidEntry entry){
    		this.client = client;
    		this.guid = guid;
    		this.entry = entry;
    	}
    	
    	public String call() throws Exception {
    		String result = "";
    		try{
    			result = client.fieldRead(guid, "hi", entry);
    		}catch(Exception e){
    			//e.printStackTrace();
    		}
    		return result;
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
		int fraction = NUM_CLIENT - Integer.parseInt(args[2]);
		
		CapacityTestClient[] clients = new CapacityTestClient[NUM_CLIENT];
		
		for (int index=0; index<10; index++){
			UniversalTcpClient client = new UniversalTcpClient(EC2_ADDRESS, 24398);
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
    	sendRequests(TOTAL, rate, clients, fraction);
    	long t2 = System.currentTimeMillis();
    	long elapsed = t2 - t1;
    	
    	System.out.println("It takes "+elapsed+"ms.");
    	
    	TOTAL = TOTAL * fraction / NUM_CLIENT;
    	
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
    	System.out.println("There are "+failed+" requests failed.");
    	System.out.println("The average latency is "+total/latency.size()+"ms");
    	System.out.println("The start point is:"+(start/1000));
    	
    	System.exit(0);		
	}
}
