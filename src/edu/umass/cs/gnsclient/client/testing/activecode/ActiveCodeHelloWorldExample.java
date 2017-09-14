package edu.umass.cs.gnsclient.client.testing.activecode;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.json.JSONException;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;
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
	 * @throws IOException 
	 * @throws ClientException 
	 * @throws JSONException 
	 */
	public static void main(String[] args) throws IOException, ClientException, JSONException {
		
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
		final GNSClient client = new GNSClient();
		
		final String ACCOUNT_GUID_PREFIX = "GNS_ACCOUNT_";
		final String ACCOUNT_GUID = ACCOUNT_GUID_PREFIX + name;
		final String PASSWORD = "";
		edu.umass.cs.gnsclient.client.util.GuidEntry entry = null;
		// create an account	
		try {
			entry = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_GUID, PASSWORD);
			ObjectOutputStream output = new ObjectOutputStream(new FileOutputStream(new File("guid")));
			entry.writeObject(output);
			output.flush();
			output.close();
		} catch (Exception e) {
			e.printStackTrace();
			// The guid is already created, try to read from a local file
			try {
				ObjectInputStream input = new ObjectInputStream(new FileInputStream(new File("guid")));
				entry = new GuidEntry(input);
				input.close();
			} catch (IOException | EncryptionException ie) {
				ie.printStackTrace();
			}
		}
		
		
		String field = "someField";
		String value = "original value";
		
		String depth_field = "depthField";
		String depth_result = "Depth query succeeds";
		
		// set up a field
		client.execute(GNSCommand.fieldUpdate(entry, field, value));
		client.execute(GNSCommand.fieldUpdate(entry,depth_field, depth_result));

		// clear code for both read and write action
		client.execute(GNSCommand.activeCodeClear(entry.getGuid(), ActiveCode.READ_ACTION, entry));
		client.execute(GNSCommand.activeCodeClear(entry.getGuid(), ActiveCode.WRITE_ACTION, entry));
		
		// get the value of the field
		String response = client.execute(GNSCommand.fieldRead(entry, field)).getResultJSONObject().getString(field); 
		
		System.out.println("Before the code is deployed, the value of field("+field+") is "+response);
		
		// read in the code as a string
		final String code =  new String(Files.readAllBytes(Paths.get(codeFile)));
		
		// set up the code for on read operation
		if(isRead){
			if(update){
				client.execute(GNSCommand.activeCodeSet(entry.getGuid(), ActiveCode.READ_ACTION, code, entry));
			}
		} else {
			if(update){
				client.execute(GNSCommand.activeCodeSet(entry.getGuid(), ActiveCode.WRITE_ACTION, code, entry));
			}
		}
		
		// get the value of the field again
		response = client.execute(GNSCommand.fieldRead(entry, field)).getResultJSONObject().getString(field);
		System.out.println("After the code is deployed, the value of field("+field+") is "+response);
		
		ObjectOutputStream output = new ObjectOutputStream(new FileOutputStream(new File("guid")));
		entry.writeObject(output);
		output.flush();
		output.close();
		
		System.exit(0);
	}
}
