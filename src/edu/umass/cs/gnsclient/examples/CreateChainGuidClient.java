package edu.umass.cs.gnsclient.examples;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;

import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.BasicUniversalTcpClient;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.UniversalTcpClient;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.client.util.SHA1HashFunction;
import edu.umass.cs.gnsclient.exceptions.GnsException;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnscommon.utils.ByteUtils;

public class CreateChainGuidClient {
	//private final static String EC2_ADDRESS = "52.88.106.121";
	private static String ACCOUNT_ALIAS = "@gigapaxos.net";
	private static UniversalTcpClient client;
	private static int NUM_CLIENT = 100;
	private final static String filename = "/home/ubuntu/chain.js"; //"/Users/gaozy/GNS/test.js"; //
	private final static String key_folder = "/home/ubuntu/gns_key/"; //"/Users/gaozy/GNS/gns_key/"; //
	
	public static void main(String[] args) throws IOException,
    InvalidKeySpecException, NoSuchAlgorithmException, 
    InvalidKeyException, SignatureException, Exception {
		String address = args[0];
		int node = Integer.parseInt(args[1]);
		NUM_CLIENT = Integer.parseInt(args[2]);
		
		//Read in the code and serialize
		String code = new String(Files.readAllBytes(Paths.get(filename)));
		String code64 = Base64.encodeToString(code.getBytes("utf-8"), true);
				
		client = new UniversalTcpClient(address, 24398, true);
		
		String lastGuid = "";
		GuidEntry guidAccount = null;
		
		for (int i=0; i<NUM_CLIENT; i++){
			
			try{
				guidAccount = lookupOrCreateAccountGuid(client, "test"+(node*1000+i)+ACCOUNT_ALIAS, "password");
				//System.out.println("test"+(node*1000+i)+ACCOUNT_ALIAS+":"+guidAccount.getGuid());
				
				KeyPairUtils.writePrivateKeyToPKCS8File(guidAccount.getPrivateKey(), key_folder+"test"+(node*1000+i) );
			}catch (Exception e) {
			      //System.out.println("Exception during accountGuid creation: " + e);
			      System.exit(1);
			}
			
			String guid = client.lookupGuid("test"+(node*1000+i)+ACCOUNT_ALIAS);
			
			JSONObject json = new JSONObject("{\"nextGuid\":\"gao\",\"cnt\":1}");
			client.update(guidAccount, json);
			//System.out.println("The last guid is "+lastGuid);
			client.fieldUpdate(guidAccount, "nextGuid", lastGuid);
			
			client.activeCodeClear(guid, "read", guidAccount);
			JSONObject result = client.read(guidAccount);
		    //System.out.println("Retrieved JSON from guid: " + result.toString());

    		client.activeCodeSet(guid, "read", code64, guidAccount);
    		
    		lastGuid = guidAccount.getGuid();
    		
		}
		
		// For warming up only
		long t = System.nanoTime();
		for (int i=0; i<1000; i++){
			client.fieldRead(guidAccount, "nextGuid");
		}
		
		t = System.nanoTime();
		for (int i=0; i<1000; i++){
			client.fieldRead(guidAccount, "nextGuid");
		}
		long eclapsed = System.nanoTime() - t;
		System.out.println(NUM_CLIENT+" "+eclapsed/1000000/1000);
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


