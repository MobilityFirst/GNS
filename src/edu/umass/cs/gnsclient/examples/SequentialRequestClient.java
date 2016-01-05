package edu.umass.cs.gnsclient.examples;

import java.io.IOException;
import java.math.BigInteger;
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

import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.BasicUniversalTcpClient;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.UniversalTcpClient;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.client.util.SHA1HashFunction;
import edu.umass.cs.gnsclient.exceptions.GnsException;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnscommon.utils.ByteUtils;

public class SequentialRequestClient {
	private static String ACCOUNT_ALIAS = "@gigapaxos.net";
	private static UniversalTcpClient client;
	private final static String filename = "/home/ubuntu/test.js";//"/Users/zhaoyugao/Documents/ActiveCode/Activecode/test.js";//
	private static ArrayList<Long> latency = new ArrayList<Long>();
	
	public static void main(String[] args) throws IOException,
    InvalidKeySpecException, NoSuchAlgorithmException, 
    InvalidKeyException, SignatureException, Exception {
		String address = args[0];
		
		boolean flag = Boolean.parseBoolean(args[1]);
		int size = 10000;
		if(args.length > 2){
			size = Integer.parseInt(args[2]);
		}
		size = size*8;
		
		String code = new String(Files.readAllBytes(Paths.get(filename)));		
		//Read in the code and serialize
		String code64 = Base64.encodeToString(code.getBytes("utf-8"), true);
		
		client = new UniversalTcpClient(address, 24398, true);
		GuidEntry guidAccount = null;
		try{
			guidAccount = lookupOrCreateAccountGuid(client, "test"+ACCOUNT_ALIAS, "password");
			//System.out.println("test"+ACCOUNT_ALIAS+":"+guidAccount.getGuid());
			
			//KeyPairUtils.writePrivateKeyToPKCS8File(guidAccount.getPrivateKey(), key_folder+"test"+(node*10+i) );
		}catch (Exception e) {
		      System.out.println("Exception during accountGuid creation: " + e);
		      System.exit(1);
		}
		
		String guid = client.lookupGuid("test"+ACCOUNT_ALIAS);
		
		JSONObject json = new JSONObject("{\"hi\":\"hello\", \"hehe\":\"hello\"}");
		client.update(guidAccount, json);
		
		SecureRandom random = new SecureRandom();
		String str = new BigInteger(size, random).toString(32);
		//System.out.println("The string length is "+str.length());
		client.fieldUpdate(guidAccount.getGuid(), "hi", str, guidAccount);
		String result = client.fieldRead(guidAccount, "hi");
		System.out.println("The response after setting active code is"+result);
		client.activeCodeClear(guid, "read", guidAccount);
	    if(flag){
		    client.activeCodeSet(guid, "read", code64, guidAccount);
	    }
	    
	    result = client.fieldRead(guidAccount, "hi");
	    System.out.println("The response after setting active code is"+result);
	    
	    for (int i=0; i<5000; i++){
	    	long t1 = System.nanoTime();
	    	client.fieldRead(guidAccount.getGuid(), "hi", guidAccount);
	    	long t2 = System.nanoTime();
	    	long elapsed = t2 - t1;
	    	latency.add(elapsed);
	    }
	    
	    Collections.sort(latency);
	    
	    System.out.println(latency.get(latency.size()/2)+" ");
	    long total = 0;
	    for (long lat:latency){
	    	total += lat;
	    }
	    System.out.println(total/latency.size());
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
	  GuidEntry guidEntry = KeyPairUtils.getGuidEntry(client.getGnsRemoteHost() + ":" + client.getGnsRemotePort(), name);
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
	    } catch (GnsException e) {
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
