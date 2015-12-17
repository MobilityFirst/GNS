package edu.umass.cs.gnsclient.examples;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;

import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.BasicUniversalTcpClient;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.client.util.SHA1HashFunction;
import edu.umass.cs.gnsclient.exceptions.GnsException;
import edu.umass.cs.gnscommon.utils.ByteUtils;


public class MultiGuidClient {
	private static String ACCOUNT_ALIAS = "@cs.umass.edu";
	private static BasicUniversalTcpClient client;
	
	public static void main(String[] args) throws IOException,
    InvalidKeySpecException, NoSuchAlgorithmException, GnsException,
    InvalidKeyException, SignatureException, Exception {
		
		InetSocketAddress address = new InetSocketAddress("0.0.0.0", 24398);
		
		client = new BasicUniversalTcpClient(address.getHostName(), address.getPort());
		for (int i=0; i<2; i++){
			GuidEntry guid = null;
			try{
				guid = lookupOrCreateAccountGuid(client, "test"+i+ACCOUNT_ALIAS, "password");
			}catch (Exception e) {
			      System.out.println("Exception during accountGuid creation: " + e);
			      System.exit(1);
			}
			
			JSONObject json = new JSONObject("{\"hi\":\"hello\"}");
			client.update(guid, json);
			
			JSONObject result = client.read(guid);
		    System.out.println("Retrieved JSON from guid: " + result.toString());
		}
	    
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


