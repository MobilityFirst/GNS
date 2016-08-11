package edu.umass.cs.gnsclient.client.testing.activecode;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.ActiveCode;

/**
 * This is an hello world example for ActiveGNS.
 * In this example, it shows you how to create an account, create a field
 * on the account, retrieve the value of the field, and deploy your own
 * code on ActiveGNS.
 * 
 * @author gaozy
 *
 */
public class ActiveCodeHelloWorldExample {
	
	
	
	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		
		String codeFile = "scripts/activeCode/noop.js";
		if(args.length > 0){
			codeFile = args[0];
		}
				
		// create a client
		final GNSClientCommands client = new GNSClientCommands();
		
		final String ACCOUNT_GUID_PREFIX = "GNS_ACCOUNT";
		final String PASSWORD = "";
		
		// create an account
		final edu.umass.cs.gnsclient.client.util.GuidEntry entry = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_GUID_PREFIX, PASSWORD);
		
		String field = "someField";
		String value = "someValue";
		
		// set up a field
		client.fieldUpdate(entry,  field, value);
		
		// get the value of the field
		String response = client.fieldRead(entry, field);
		
		System.out.println("Before the code is deployed, the value of field("+field+") is "+response);
		
		// read in the code as a string
		final String code = new String(Files.readAllBytes(Paths.get(codeFile)));
		
		// set up the code for on read operation
		client.activeCodeSet(entry.getGuid(), ActiveCode.READ_ACTION, code, entry);
		
		// get the value of the field again
		response = client.fieldRead(entry, field);
		System.out.println("After the code is deployed, the value of field("+field+") is "+response);
		
		ObjectOutputStream output = new ObjectOutputStream(new FileOutputStream(new File("guid")));
		entry.writeObject(output);
		output.flush();
		output.close();
		
		System.exit(0);
	}
}
