package edu.umass.cs.gnsclient.examples;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;

import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.UniversalTcpClient;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.exceptions.GnsException;
import edu.umass.cs.gnscommon.utils.Base64;

public class CreateBenignGuidClient {
		private final static String ACCOUNT_ALIAS = "@gigapaxos.net";
		private final static String filename = "/home/ubuntu/test.js";
		
		private static int NUM_CLIENT;
	    
	    public static void main(String[] args) throws IOException,
	    InvalidKeySpecException, NoSuchAlgorithmException, GnsException,
	    InvalidKeyException, SignatureException, Exception {
	    	String address = args[0];
			int node = Integer.parseInt(args[1]); 			
			int BENIGN = Integer.parseInt(args[2]);	
			System.out.println("There are "+BENIGN+"/"+NUM_CLIENT+" clients.");
			
			String code = new String(Files.readAllBytes(Paths.get(filename)));
			String code64 = Base64.encodeToString(code.getBytes("utf-8"), true);
			
			UniversalTcpClient client = new UniversalTcpClient(address, 24398, true);
	    	
			for (int index=0; index<BENIGN; index++){			
				String account = "test"+(node*1000+index)+ACCOUNT_ALIAS;
				
				GuidEntry accountGuid = KeyPairUtils.getGuidEntry(address + ":" + client.getGnsRemotePort(), account);
				
				client.activeCodeSet(accountGuid.getGuid(), "read", code64, accountGuid);
				System.out.println("Create benign user "+index);
			}
			
			System.out.println("Benign users, all set!");
			System.exit(0);
	    }
	    
}
