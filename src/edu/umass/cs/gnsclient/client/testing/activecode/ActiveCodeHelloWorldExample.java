package edu.umass.cs.gnsclient.client.testing.activecode;

import java.io.File;
import java.io.FileOutputStream;
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
		
		boolean update = true;
		if(System.getProperty("update")!=null){
			update = Boolean.parseBoolean(System.getProperty("update"));
		}
		
		boolean isRead = true;
		if(System.getProperty("isRead")!=null){
			isRead = Boolean.parseBoolean(System.getProperty("isRead"));
		}
		
		String name = "benign";
		if(System.getProperty("name")!=null){
			name=System.getProperty("name");
		}
		
		String codeFile = "scripts/activeCode/noop.js";
		if(System.getProperty("codeFile")!=null){
			codeFile = System.getProperty("codeFile");
		}
		
		if(args.length > 0){
			codeFile = args[0];
		}
				
		// create a client
		final GNSClientCommands client = new GNSClientCommands();
		
		final String ACCOUNT_GUID_PREFIX = "GNS_ACCOUNT_";
		final String ACCOUNT_GUID = ACCOUNT_GUID_PREFIX + name;
		final String PASSWORD = "";
		
		// create an account
		final edu.umass.cs.gnsclient.client.util.GuidEntry entry = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_GUID, PASSWORD);
		
		String field = "someField";
		String value = "original value";
		
		String depth_field = "depthField";
		String depth_result = "Depth query succeeds";
		
		// set up a field
		client.fieldUpdate(entry, field, value);
		client.fieldUpdate(entry, depth_field, depth_result);
		
		// clear code for both read and write action
		client.activeCodeClear(entry.getGuid(), ActiveCode.READ_ACTION, entry);
		client.activeCodeClear(entry.getGuid(), ActiveCode.WRITE_ACTION, entry);
		
		// get the value of the field
		String response = client.fieldRead(entry, field);
		
		System.out.println("Before the code is deployed, the value of field("+field+") is "+response);
		
		// read in the code as a string
		final String code = new String(Files.readAllBytes(Paths.get(codeFile)));
		
		// set up the code for on read operation
		if(isRead){
			if(update)
				client.activeCodeSet(entry.getGuid(), ActiveCode.READ_ACTION, code, entry);
		} else {
			if(update)
				client.activeCodeSet(entry.getGuid(), ActiveCode.WRITE_ACTION, code, entry);
		}
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
