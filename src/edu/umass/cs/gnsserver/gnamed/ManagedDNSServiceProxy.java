package edu.umass.cs.gnsserver.gnamed;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.ActiveCode;

/**
 * @author gaozy
 *
 */
public class ManagedDNSServiceProxy implements Runnable {
	
	protected final static String RECORD_FIELD = "record";
	protected final static String TTL_FIELD = "ttl";
	
	private final static String ACTION_FIELD = "action";
	private final static String GUID_FIELD = "guid";
	private final static String CODE_FIELD = "code";
	private final static String USERNAME_FIELD = "username";
	
	private final static String A_RECORD_FIELD = "A";
	private final static String NS_RECORD_FIELD = "NS";
	
	private enum Actions {
	    CREATE("create"),
	    UPDATE_RECORD("update_record"),
	    UPDATE_CODE("update_code"),
	    // remove the code
	    REMOVE_CODE("remove_code"),
	    // delete the record
	    DELETE_RECORD("delete_record")
	    ;

	    private final String text;

	    /**
	     * @param text
	     */
	    private Actions(String text) {
	        this.text = text;
	    }

	    /**
	     * @see java.lang.Enum#toString()
	     */
	    @Override
	    public String toString() {
	        return text;
	    }
	}
	
	private static GNSClientCommands client;
	private static GuidEntry accountGuid;
	
	private final static int default_ttl = 30;
	private final static String DOMAIN = "activegns.org.";
	private final static String NS1 = "ns1."+DOMAIN;
	private final static String NS2 = "ns2."+DOMAIN;
	private final static String NS3 = "ns3."+DOMAIN;
	
	private final static String NS1_ADDRESS = "52.43.241.146";
	private final static String NS2_ADDRESS = "52.44.1.207";
	private final static String NS3_ADDRESS = "52.203.144.175";
	
	private final static ExecutorService executor = Executors.newFixedThreadPool(10);
	
	private ManagedDNSServiceProxy(){
		
		try {
			client = new GNSClientCommands();
		} catch (IOException e) {
			e.printStackTrace();
		}
		String guid_file = null;
		if(System.getProperty("guid_file")!=null){
			guid_file = System.getProperty("update");
		}
		if (guid_file==null){
			// create account guid with username admin 
			try {
				accountGuid = GuidUtils.lookupOrCreateAccountGuid(client, "admin",
						"password", true);
				ObjectOutputStream output = new ObjectOutputStream(new FileOutputStream(new File("proxy_guid")));
				accountGuid.writeObject(output);
				output.flush();
				output.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else{
			try {
				ObjectInputStream input = new ObjectInputStream(new FileInputStream(new File(guid_file)));
				accountGuid = new GuidEntry(input);
				input.close();
			} catch (IOException | EncryptionException e) {
				e.printStackTrace();
			}
		}
		
		try {
			deployDomain();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void deployDomain() throws Exception {
		int ttl = 0;
		
		GuidEntry guid = GuidUtils.lookupOrCreateGuid(client, accountGuid, DOMAIN);
		
		List<String> records = new ArrayList<String>();
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("conf/activeCode/records")));
		String line = reader.readLine();
		while(line != null){
            records.add(line);
            line = reader.readLine();
        } 
		reader.close();
		
		System.out.println("The record list is "+records);
		updateRecord(guid, A_RECORD_FIELD, records, ttl);
		
		// Update NS records
		List<JSONArray> ns_records = new ArrayList<JSONArray>();
		ns_records.add(new JSONArray().put(NS1).put(NS1_ADDRESS));
		ns_records.add(new JSONArray().put(NS2).put(NS2_ADDRESS));
		ns_records.add(new JSONArray().put(NS3).put(NS3_ADDRESS));
		
		//updateRecord(guid, NS_RECORD_FIELD, ns_records, 120);
		JSONObject recordObj = new JSONObject();
		recordObj.put(RECORD_FIELD, ns_records);
		recordObj.put(TTL_FIELD, 120);
		
		client.execute(GNSCommand.fieldUpdate(guid, NS_RECORD_FIELD, recordObj));
		
		System.out.println("Create record for "+DOMAIN);
	}
	
	private static JSONObject recordToCreate(List<String> ips, int ttl) {
		JSONObject recordObj = new JSONObject();
		JSONArray records = new JSONArray();
		for (String ip:ips){
			records.put(ip);
		}
		
		try {
			recordObj.put(RECORD_FIELD, records);
			recordObj.put(TTL_FIELD, ttl);
		} catch (JSONException e) {
			
		}
		return recordObj;
	}
	
	private static void updateRecord(GuidEntry entry, String fieldToUpdate, List<String> ips, int ttl){
		System.out.println("Ready to update record for "+entry+" field:"+fieldToUpdate);
		JSONObject recordObj = recordToCreate(ips, ttl);
		try {
			client.execute(GNSCommand.fieldUpdate(entry, fieldToUpdate, recordObj));
		} catch (ClientException | IOException e) {
			e.printStackTrace();
			// The update failed			
		}	
	}
	
	private static void updateCode(GuidEntry entry, byte[] code) throws ClientException, IOException {
	  client.activeCodeSet(entry.getGuid(), ActiveCode.READ_ACTION, code, entry);
	}
	
	
	private static GuidEntry createGuidEntryForDomain(String domain) throws Exception{		
		return GuidUtils.lookupOrCreateGuid(client, accountGuid, domain);
	}
	
	
	private static void deleteRecord(GuidEntry entry){
		// clear the code
		removeCode(entry);
		
		try {
			client.execute(GNSCommand.fieldRemove(entry, A_RECORD_FIELD));
		} catch (ClientException | IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void removeCode(GuidEntry entry){
		try {
			client.activeCodeClear(entry.getGuid(), ActiveCode.READ_ACTION, entry);
		} catch (ClientException | IOException e) {
			e.printStackTrace();
		}
	}
	
	private static String serializeGuid(GuidEntry entry){
		try {
		     ByteArrayOutputStream bo = new ByteArrayOutputStream();
		     ObjectOutputStream so = new ObjectOutputStream(bo);
		     entry.writeObject(so);
		     so.flush();
		     so.close();
		     return Base64.getEncoder().encodeToString(bo.toByteArray());
		 } catch (Exception e) {
			 e.printStackTrace();
		     return null;
		 }
	}
	
	private static GuidEntry deserializeGuid(String key) {
		try {
		     byte b[] = Base64.getDecoder().decode(key); 
		     ObjectInputStream si = new ObjectInputStream(new ByteArrayInputStream(b));
		     GuidEntry guid = new GuidEntry(si);
		     si.close();
		     return guid;
		 } catch (Exception e) {
		     e.printStackTrace();
		     return null;
		 }
	}
	
	private static JSONObject handleRequest(JSONObject req){
		JSONObject result = new JSONObject();
		try {
			Actions action = Actions.valueOf(req.getString(ACTION_FIELD).toUpperCase());
			switch(action){
				case CREATE:{
						String username = req.getString(USERNAME_FIELD);
						String subdomain = username+"."+DOMAIN;
						GuidEntry entry = createGuidEntryForDomain(subdomain);
						String guid = serializeGuid(entry);
						result.put(GUID_FIELD, guid);
				}
				break;
				case UPDATE_RECORD:{
					String record = req.getString(RECORD_FIELD);
					GuidEntry guid = deserializeGuid(req.getString(GUID_FIELD));
					List<String> ips = Arrays.asList(record.split("\\n"));
					updateRecord(guid, A_RECORD_FIELD, ips, default_ttl);		
				}
				break;
				case UPDATE_CODE:{
					GuidEntry guid = deserializeGuid(req.getString(GUID_FIELD));
					String code = req.getString(CODE_FIELD);
					updateCode(guid, code.getBytes("UTF-8"));
				}
				case REMOVE_CODE:{
					GuidEntry guid = deserializeGuid(req.getString(GUID_FIELD));
					removeCode(guid);
				}
				break;
				case DELETE_RECORD:{
					GuidEntry guid = deserializeGuid(req.getString(GUID_FIELD));
					deleteRecord(guid);
				}
				break;
				default:
					break;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return result;
	}
	
	@Override
	public void run() {
		System.out.println("DNS proxy starts running ...");
		ServerSocket listener = null;
		try {
			listener = new ServerSocket(9090);
		} catch (IOException e) {
			
		}
		try{
			while(true){
				try {
					Socket socket = listener.accept();
					executor.execute(new UpdateTask(socket));
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}finally{
			try {
				listener.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	final class UpdateTask implements Runnable{
		Socket sock;
		
		UpdateTask(Socket sock){
			this.sock = sock;
		}
		
		@Override
		public void run() {
			try {
				BufferedReader input = new BufferedReader(new InputStreamReader(sock.getInputStream()));
				String queryString = input.readLine();
				JSONObject request = new JSONObject(queryString);
				System.out.println("Recived query from frontend:"+request.toString());
				JSONObject response = handleRequest(request);
				if(response != null){
					PrintWriter out = new PrintWriter(sock.getOutputStream());
					out.println(response.toString());
					out.flush();
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (JSONException e) {
				e.printStackTrace();
			} finally{
				try {
					sock.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args){
		new Thread(new ManagedDNSServiceProxy()).start();
	}
}
