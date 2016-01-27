package edu.umass.cs.gnsclient.examples;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.concurrent.ThreadPoolExecutor;

import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.UniversalTcpClient;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.exceptions.GnsException;

public class CapacityTestSequentialClient {
		private final static String ACCOUNT_ALIAS = "@gigapaxos.net";
		public static ArrayList<Long> latency = new ArrayList<Long>();
		public static ArrayList<Long> mal_request = new ArrayList<Long>();
	    
	    private static int NUM_CLIENT = 0;
	    public static final int DURATION = 60;
	    public static final int INTERVAL = 5;
	    public static final int MAL_INTERVAL = 200;
	    
	    protected String guid;
	    protected GuidEntry entry;	  
	    
	    public static void main(String[] args) throws IOException,
	    InvalidKeySpecException, NoSuchAlgorithmException, GnsException,
	    InvalidKeyException, SignatureException, Exception {
	    	String address = args[0];
			int node = Integer.parseInt(args[1]); 
			NUM_CLIENT =  Integer.parseInt(args[2]);
			int MAL = Integer.parseInt(args[3]);
			
			SingleClient[] clients = new SingleClient[NUM_CLIENT];
			UniversalTcpClient client = new UniversalTcpClient(address, 24398, true);
			
			for (int index=0; index<NUM_CLIENT; index++){			
				String account = "test"+(node*1000+index)+ACCOUNT_ALIAS;
				System.out.println("The account is "+account);
			
				//String guid = client.lookupGuid(account);
				
				GuidEntry accountGuid = KeyPairUtils.getGuidEntry(address + ":" + client.getGnsRemotePort(), account);
				String guid = accountGuid.getGuid();
			
				System.out.println("The GUID is "+guid);
				if (index < MAL){
					clients[index] = new SingleClient(client, accountGuid, false);
				}else{
					clients[index] = new SingleClient(client, accountGuid, true);
				}
			}
			
			Thread[] threadPool = new Thread[NUM_CLIENT];
			
			for (int i=0; i<NUM_CLIENT; i++){
				threadPool[i] = new Thread(clients[i]);
				threadPool[i].start();
			}
			
			for (int i=0; i<NUM_CLIENT; i++){
				threadPool[i].join();
			}
			
			
	    }
	    
}
