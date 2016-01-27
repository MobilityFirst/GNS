package edu.umass.cs.gnsclient.examples;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
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

public class CapacityTestSequentialClient {
		private final static String ACCOUNT_ALIAS = "@gigapaxos.net";
		public static ArrayList<Long> latency = new ArrayList<Long>();
		public static ArrayList<Long> mal_request = new ArrayList<Long>();
		private final static int MALICIOUS_EVERY_FEW_CLIENTS = 5;
	    
		private static int NUM_THREAD = 100;
	    private static int NUM_CLIENT = 0;
	    public static final int DURATION = 10;
	    public static final int INTERVAL = 5;
	    public static final int MAL_INTERVAL = 200;
	    private static SingleClient[] clients;
	    private static ThreadPoolExecutor executorPool;  
	    
	    public static void main(String[] args) throws IOException,
	    InvalidKeySpecException, NoSuchAlgorithmException, GnsException,
	    InvalidKeyException, SignatureException, Exception {
	    	String address = args[0];
			int node = Integer.parseInt(args[1]); 			
			int fraction = Integer.parseInt(args[2]);			
			if(fraction > MALICIOUS_EVERY_FEW_CLIENTS){
				System.out.println("The fraction of malicious users must lie between 0 to 5 (0%~100%).");
				System.exit(0);
			}
			fraction = MALICIOUS_EVERY_FEW_CLIENTS - fraction;
			NUM_CLIENT =  Integer.parseInt(args[3]);
			
			System.out.println("There are "+NUM_CLIENT+" clients, and "+fraction+"/5 are malicious.");
			
			clients = new SingleClient[NUM_CLIENT];
			UniversalTcpClient client = new UniversalTcpClient(address, 24398, true);
			executorPool = new ThreadPoolExecutor(NUM_THREAD, NUM_THREAD, 0, TimeUnit.SECONDS, 
		    		new LinkedBlockingQueue<Runnable>(), new MyThreadFactory() );
	    	executorPool.prestartAllCoreThreads();
	    	
			for (int index=0; index<NUM_CLIENT; index++){			
				String account = "test"+(node*1000+index)+ACCOUNT_ALIAS;
				//System.out.println("The account is "+account);
				
				GuidEntry accountGuid = KeyPairUtils.getGuidEntry(address + ":" + client.getGnsRemotePort(), account);
				//String guid = accountGuid.getGuid();
			
				//System.out.println("The GUID is "+guid);
				if (index%MALICIOUS_EVERY_FEW_CLIENTS < fraction){
					System.out.println("Set to benign "+index);
					clients[index] = new SingleClient(client, accountGuid, false);
				} else {
					System.out.println("Set to malicious "+index);
					clients[index] = new SingleClient(client, accountGuid, true);
				}
				
			}
			
			//Thread[] threadPool = new Thread[NUM_CLIENT];
			long start = System.currentTimeMillis();
			
			for (int i=0; i<NUM_CLIENT; i++){
				//threadPool[i] = new Thread(clients[i]);
				//threadPool[i].start();
				executorPool.execute(clients[i]);
			}
			
			int t = 0;
			int received = 0;
			int max = 0;
			int thruput = 0;
			
			while (executorPool.getCompletedTaskCount() < NUM_CLIENT){
				thruput = latency.size() - received;
				if(max<thruput){
					max = thruput;
				}
				System.out.println(t+" Throuput:"+thruput+" reqs/sec" );
				received = latency.size();
				t++;
				try{
					Thread.sleep(1000);
				}catch(InterruptedException e){
					e.printStackTrace();
				}
			}
									
			long eclapsed = System.currentTimeMillis()-start;
			System.out.println("It takes "+eclapsed+"ms to send all the requests");
			System.out.println("The maximum throuput is "+max+" reqs/sec, and the average throughput is "+(received/eclapsed)+" req/sec.");
			
			System.exit(0);
	    }
	    
}
