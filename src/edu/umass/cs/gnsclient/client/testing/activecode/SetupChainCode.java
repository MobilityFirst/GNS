package edu.umass.cs.gnsclient.client.testing.activecode;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GUIDUtilsHTTPClient;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.ActiveCode;

public class SetupChainCode {
	
	private static GNSClientCommands client = null;
	private static GuidEntry[] entries;
	private static String ACCOUNT_GUID_PREFIX = "ACCOUNT_GUID_";
	private static String PASSWORD = "";
	
	private final static String targetGuidField = "someField";
	private final static String successResult = "Depth query succeeds!";
	
	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception{
		int depth = 1;
		if(System.getProperty("depth")!=null){
			depth = Integer.parseInt(System.getProperty("depth"));
		}
		
		String codeFile = "scripts/activeCode/depth.js";
		if(System.getProperty("codeFile")!=null){
			codeFile = System.getProperty("codeFile");
		}
		
		boolean isRead = true;
		if(System.getProperty("isRead") != null){
			isRead = Boolean.parseBoolean(System.getProperty("isRead"));
		}
		
		int numChains = 1;
		if(System.getProperty("numChains")!=null){
			numChains = Integer.parseInt(System.getProperty("numChains"));
		}
		
		byte[] code = Files.readAllBytes(Paths.get(codeFile));
		
		client = new GNSClientCommands();	
		
		for (int j=0; j<numChains; j++){
			entries = new GuidEntry[depth];
			
			for (int i=0; i<depth; i++){
				entries[i] = GUIDUtilsHTTPClient.lookupOrCreateAccountGuid(
						client, ACCOUNT_GUID_PREFIX +(j*1000+i), PASSWORD);
			}
			
			String[] nextGuid = new String[depth];
			for(int i=0; i<depth-1; i++){
				nextGuid[i] = entries[i+1].getGuid();
			}
			nextGuid[depth-1] = successResult;
			
			for(int i=0; i<depth; i++){
				if(i==0)
					client.activeCodeClear(entries[i].getGuid(), ActiveCode.WRITE_ACTION, entries[i]);
				client.activeCodeClear(entries[i].getGuid(), ActiveCode.READ_ACTION, entries[i]);
			}
			
			for(int i=0; i<depth; i++){
				client.fieldUpdate(entries[i], targetGuidField, nextGuid[i]);
			}
						
			
			Thread.sleep(1000);
			
			for(int i=0; i<depth; i++){
				if(!isRead && i==0)
					client.activeCodeSet(entries[i].getGuid(), ActiveCode.WRITE_ACTION, code, entries[i]);
				else
					client.activeCodeSet(entries[i].getGuid(), ActiveCode.READ_ACTION, code, entries[i]);
			}
			Thread.sleep(1000);
			
			for(int i=0; i<depth; i++){
				String response = client.fieldRead(entries[i], targetGuidField);			
				//assert(response.equals(successResult));
				System.out.println("Response is "+response+", succeeds for "+entries[i].getEntityName()+"("+entries[i].getGuid()+")");
				Thread.sleep(1000);
			}
			System.out.println("Depth query code chain has been successfully set up!");
			
			// save all guids
			for(int i=0; i<depth; i++){
				ObjectOutputStream output = new ObjectOutputStream(new FileOutputStream(new File("guid"+(j*1000+i) )));
				entries[i].writeObject(output);
				output.flush();
				output.close();
			}
		}	
		System.exit(0);
	}
}
