package edu.umass.cs.gnsclient.examples;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
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

/**
 * @author gaozy
 *
 */
public class CreateMaliciousGuidClient {
		private final static String ACCOUNT_ALIAS = "@gigapaxos.net";
		private final static String filename = "../scripts/activeCode/mal.js";
		
		private static int NUM_CLIENT;
	    
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
			int MALICIOUS = Integer.parseInt(args[2]);
			NUM_CLIENT = Integer.parseInt(args[3]);
			
			System.out.println("There are "+NUM_CLIENT+" clients.");
			
			String code = new String(Files.readAllBytes(Paths.get(filename)));
			String code64 = Base64.encodeToString(code.getBytes("utf-8"), true);
			
			UniversalTcpClient client = new UniversalTcpClient(address, 24398, true);
			
			for (int index=0; index<MALICIOUS; index++){			
				String account = "test"+(node*1000+index)+ACCOUNT_ALIAS;
				
				GuidEntry accountGuid = KeyPairUtils.getGuidEntry(address + ":" + client.getGnsRemotePort(), account);
				
				client.activeCodeSet(accountGuid.getGuid(), "read", code64, accountGuid);
				System.out.println("Create benign user "+index);
			}
			
			System.out.println("Malicious users, all set!");
			
			Socket socket = new Socket("128.119.245.5", 60001);
	    	PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
	    	out.println(node+"\n");
	    	socket.close();
	    	
			System.exit(0);
	    }
	    
}
