package edu.umass.cs.gnsclient.examples;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.UniversalTcpClient;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.exceptions.GnsException;

/**
 * @author gaozy
 *
 */
public class CapacityTestChainClient {
		private final static String ACCOUNT_ALIAS = "@gigapaxos.net";
			    
		private static int NUM_THREAD = 100;
	    private static int NUM_CLIENT = 0;
	    private static SingleClient[] clients;
	    private static ThreadPoolExecutor executorPool;  
	    
	    /**
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
			int node = Integer.parseInt(args[1]); 			
			int BENIGN = Integer.parseInt(args[2]);	
			NUM_CLIENT = Integer.parseInt(args[3]);
			MessageStats.DEPTH = Integer.parseInt(args[4]);
			System.out.println("There are "+BENIGN+"/"+NUM_CLIENT+" clients, depth is "+MessageStats.DEPTH);
			
			clients = new SingleClient[NUM_CLIENT];
			UniversalTcpClient client = new UniversalTcpClient(address, 24398, true);
			executorPool = new ThreadPoolExecutor(NUM_THREAD, NUM_THREAD, 0, TimeUnit.SECONDS, 
		    		new LinkedBlockingQueue<Runnable>(), new MyThreadFactory() );
	    	executorPool.prestartAllCoreThreads();
	    	
			for (int index=0; index<NUM_CLIENT; index++){
				if (index < BENIGN){
					//create benign users
					String account = "test"+(node*1000+index)+ACCOUNT_ALIAS;
					GuidEntry accountGuid = KeyPairUtils.getGuidEntry(address + ":" + client.getGnsRemotePort(), account);				
					clients[index] = new SingleClient(client, accountGuid, false);
				} else {
					// otherwise create malicious users
					String account = "test"+(node*1000+index*10+MessageStats.DEPTH-1)+ACCOUNT_ALIAS;
					GuidEntry accountGuid = KeyPairUtils.getGuidEntry(address + ":" + client.getGnsRemotePort(), account);
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
				thruput = (MessageStats.latency.size()+MessageStats.mal_request.size()) - received;
				if(max<thruput){
					max = thruput;
				}
				System.out.println(t+" Throuput:"+thruput+" reqs/sec" );
				received = MessageStats.latency.size()+MessageStats.mal_request.size();
				t++;
				try{
					Thread.sleep(1000);
				}catch(InterruptedException e){
					e.printStackTrace();
				}
			}
						
			double eclapsed = System.currentTimeMillis()-start;
			System.out.println("It takes "+eclapsed+"ms to send all the requests");
			System.out.println("The maximum throuput is "+max+" reqs/sec, and the average throughput is "+(1000*(MessageStats.latency.size()+MessageStats.mal_request.size())/eclapsed)+" req/sec.");
			
			Socket socket = new Socket("128.119.245.5", 60001);
	    	PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
	    	out.println(node+"\n");
	    	socket.close();
			
			System.exit(0);
	    }
	    
}
