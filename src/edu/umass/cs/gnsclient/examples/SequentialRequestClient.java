package edu.umass.cs.gnsclient.examples;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.logging.Level;

import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.BasicUniversalTcpClient;
import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.client.util.SHA1HashFunction;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnscommon.utils.ByteUtils;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeGuidEntry;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;

/**
 * @author gaozy
 */
public class SequentialRequestClient {
	private static String ACCOUNT_ALIAS = "test@gigapaxos.net";
	private static GNSClient client;
	private static String filename =  "./scripts/activeCode/noop.js"; //"/Users/gaozy/WebStorm/test.js"; //
	private static ArrayList<Long> latency = new ArrayList<Long>();
	private static int numReqs = 1;
	
	private final static String guidFilename = "planetlab_key";
	protected static String getGuidFilename(){
		return guidFilename;
	}
	
	
	// FIXME: remove hard coding here
	private static final String GNS_INSTANCE = "server.gns.name";
	protected static String getDefaultGNSInstance() {
		return 
				// TODO: Need to change BasicUniversalTcpClient
				//BasicUniversalTcpClient.DEFAULT_INSTANCE;
				GNS_INSTANCE;
	}
	
	/**
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
		boolean flag = Boolean.parseBoolean(args[1]);
		
		int size = 10;
		if(args.length > 2){
			numReqs = Integer.parseInt(args[2]);
			filename = args[3];
		}
		size = size*8;
		
		ReconfigurationConfig.setConsoleHandler(Level.WARNING);
		
		String code = new String(Files.readAllBytes(Paths.get(filename)));		
		//Read in the code and serialize
		String code64 = Base64.encodeToString(code.getBytes("utf-8"), true);
		client = new GNSClient( (InetSocketAddress) null, new InetSocketAddress(address, GNSClientConfig.LNS_PORT), true); //new UniversalTcpClient(address, 24398, false);
		
		
		GuidEntry guidAccount = null;
		try{
			guidAccount = lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, "password");
		}catch (Exception e) {
			System.out.println("Exception during accountGuid creation: " + e);
			System.exit(1);
		}
		
		// write the key to a file 
		FileOutputStream fos = new FileOutputStream(getGuidFilename());
		@SuppressWarnings("resource")
		ObjectOutputStream os = new ObjectOutputStream(fos);
	    os.writeObject(new ActiveCodeGuidEntry(guidAccount));
	    
						
		JSONObject json = new JSONObject("{\"nextGuid\":\"a\", \"hehe\":\"hello\"}");
		client.update(guidAccount, json);
		
		SecureRandom random = new SecureRandom();
		String str = new BigInteger(size, random).toString(32);
		client.fieldUpdate(guidAccount.getGuid(), "nextGuid", str, guidAccount);
		
		
		client.activeCodeClear(guidAccount.getGuid(), "read", guidAccount);
	    if(flag){
		    client.activeCodeSet(guidAccount.getGuid(), "read", code64, guidAccount);
	    }
	    
	    // Warm up
	    for (int i=0; i<numReqs; i++){
	    	long t1 = System.nanoTime();
	    	client.fieldRead(guidAccount.getGuid(), "nextGuid", guidAccount);
	    	long t2 = System.nanoTime();
	    	long elapsed = t2 - t1;
	    	latency.add(elapsed);
	    }
	    latency.clear();
	    
	    for (int i=0; i<numReqs; i++){
	    	long t1 = System.nanoTime();
	    	String result = 
	    			client.fieldRead(guidAccount.getGuid(), "nextGuid", guidAccount);
	    	System.out.println("The nextGuid is "+result);
	    	long t2 = System.nanoTime();
	    	long elapsed = t2 - t1;
	    	latency.add(elapsed);
	    }
	    
	    Collections.sort(latency);
	    
	    System.out.print(latency.get(latency.size()/2)+" ");
		
	    System.exit(0);
	    
	}

	
	/**
	 * Creates and verifies an account GUID. Yes it cheats on verification
	 * using a backdoor built into the GNS server.
	 *
	 * @param client
	 * @param name
	 * @return
	 * @throws Exception
	 */
	private static GuidEntry lookupOrCreateAccountGuid(BasicUniversalTcpClient client,
	        String name, String password) throws Exception {
	  GuidEntry guidEntry = KeyPairUtils.getGuidEntry(getDefaultGNSInstance(), name);
	  if (guidEntry == null || !guidExists(client, guidEntry)) { // also handle case where it has been deleted from database
	    guidEntry = client.accountGuidCreate(name, password);
	    client.accountGuidVerify(guidEntry, createVerificationCode(name));
	    return guidEntry;
	  } else {
	    return guidEntry;
	  }
	}
	
	private static boolean guidExists(BasicUniversalTcpClient client, GuidEntry guid)
	          throws IOException {
	    try {
	      client.lookupGuidRecord(guid.getGuid());
	    } catch (Exception e) {
	      return false;
	    }
	    return true;
	  }
	
	  private static final int VERIFICATION_CODE_LENGTH = 3; // Six hex characters
	  // this is so we can mimic the verification code the server is generting
	  // AKA we're cheating... if the SECRET changes on the server side 
	  // you'll need to change it here as well
	  private static final String SECRET = "AN4pNmLGcGQGKwtaxFFOKG05yLlX0sXRye9a3awdQd2aNZ5P1ZBdpdy98Za3qcE"
	          + "o0u6BXRBZBrcH8r2NSbqpOoWfvcxeSC7wSiOiVHN7fW0eFotdFz0fiKjHj3h0ri";

	  private static String createVerificationCode(String name) {
	    return ByteUtils.toHex(Arrays.copyOf(SHA1HashFunction.getInstance().hash(name + SECRET), VERIFICATION_CODE_LENGTH));
	  }
}
