package edu.umass.cs.gnsclient.client.testing.activecode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.ActiveCode;

/**
 * This client is only used for load balancing experiment. It creates
 * a guid and save the key to a file so that the other clients on
 * planetlab could use the key. Then it update the fields to allow the
 * clients to read the values for experiment.
 * If you do not have the files for updating latency, please do not try
 * this client.
 * 
 * @author gaozy
 *
 */
public class ActivePolicyClient {
	
	private static boolean toUpdate = false;
	
	private final static String ACCOUNT_GUID_PREFIX = "ACCOUNT_GUID";
	private final static String PASSWORD = "";
	private static GuidEntry entry;
	private static GNSClientCommands client;
	
	private static List<Double> initializeField(String folder) throws IOException, JSONException, ClientException{
		File dir = new File(folder+"/latency");
		if( !dir.exists()){
			// If the folder does not exist with an error
			System.exit(1);
		}
		
		File listFile = new File(folder+"/ec2-clients");
		BufferedReader br = new BufferedReader(new FileReader(listFile));
		String line = null;
		
		int cnt = 0;
		HashMap<Integer, Double> map = new HashMap<Integer, Double>();
		
		while ((line = br.readLine()) != null) {
			//System.out.println(line);
			cnt++;
			String host = null;
			if(line.startsWith("#")){
				continue;
			} else {
				host = line.split(" ")[1];
			}
			
			System.out.println(host);
			JSONArray arr = new JSONArray();
			BufferedReader reader = new BufferedReader(new FileReader(new File(folder+"/latency/"+host)));
			for (int i=0; i<4; i++){
				double total = 0;
				String l = reader.readLine();				
				List<String> lats = Arrays.asList(l.substring(1, l.length()-1).split(","));
				for (String lat:lats){
					total += Double.parseDouble(lat);
				}
				double avg = total/lats.size();
				//System.out.println(avg);
				arr.put(avg);
				map.put(i, map.get(i)==null?avg:map.get(i)+avg);
			}
			if(toUpdate)
				client.fieldCreateList(entry, host, arr);
			reader.close();
		}
		
		System.out.println(map);
		
		br.close();
		
		List<Double> arr = new ArrayList<Double>();		
		for(int i=0; i<4; i++){
			arr.add(map.get(i)/cnt);
		}
		
		return arr;
	}
	
	private static List<String> initializeRecords(String folder) throws IOException{
		File listFile = new File(folder+"/ec2-servers");
		BufferedReader br = new BufferedReader(new FileReader(listFile));
		String line = null;
		List<String> arr = new ArrayList<String>();
		while ((line = br.readLine()) != null) {
			if (line.startsWith("#"))
				continue;
			else{
				arr.add(line.split(" ")[1]);
			}
		}
		br.close();
		
		return arr;
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception{
		
		toUpdate = Boolean.parseBoolean(args[0]);
		
		
		String folder = "/Users/gaozy/Documents/EC2"; //System.getProperty("planetlabFolder");
		if (folder.endsWith("/")){
			folder = folder.substring(0, folder.length()-1);
		}
		
		JSONArray records = new JSONArray(initializeRecords(folder));
		System.out.println(records);
				
		
		JSONArray cost = new JSONArray();
		JSONArray load = new JSONArray();
		for (int i=0; i<records.length(); i++){
			cost.put(0);
			load.put(0);
		}
		
		System.out.println("cost:"+cost);
		System.out.println("load:"+load);
		
		if(toUpdate){
			client = new GNSClientCommands();
			
			entry = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_GUID_PREFIX, PASSWORD);
			
			
			ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream("/Users/gaozy/Documents/EC2/guid"));
			entry.writeObject(out);
			out.flush();
			out.close();
						
			client.fieldCreateList(entry, "COST", cost);
			client.fieldCreateList(entry, "LOAD", load);
		}
		
		JSONArray arr = new JSONArray(initializeField(folder));
		System.out.println(arr);
		
		String codeFile = "scripts/activeCode/load-balance-cost.js";
		codeFile = "scripts/activeCode/noop.js";
		
		String code = new String(Files.readAllBytes(Paths.get(codeFile)));;
		code = code.replace("HOSTS", records.toString());
		code = code.replace("PERFORMANCE", arr.toString());
		//System.out.println(code);
		
		if(toUpdate){
			client.activeCodeClear(entry.getGuid(), ActiveCode.READ_ACTION, entry);
			//client.activeCodeSet(entry.getGuid(), ActiveCode.READ_ACTION, code, entry);
		}
		
		System.exit(0);
	}
}
