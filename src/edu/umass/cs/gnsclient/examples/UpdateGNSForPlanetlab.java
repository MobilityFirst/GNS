package edu.umass.cs.gnsclient.examples;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;

import org.json.JSONArray;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeGuidEntry;

/**
 * Used to update the latency on Active GNS server
 * 
 * @author Zhaoyu Gao
 */
public class UpdateGNSForPlanetlab {
	private static GNSClient client;
	private static GuidEntry guidEntry;
	
	private final static String pearAddress = "128.119.245.20";
	
	private final static int num_server = 6;
	
	private static void Client() throws IOException{
		client = new GNSClient((InetSocketAddress) null, new InetSocketAddress(pearAddress, GNSClientConfig.LNS_PORT), true);
		try{
			FileInputStream fis = new FileInputStream(SequentialRequestClient.getGuidFilename());
			@SuppressWarnings("resource")
			ObjectInputStream ois = new ObjectInputStream(fis);
			ActiveCodeGuidEntry activeEntry = (ActiveCodeGuidEntry) ois.readObject();
			guidEntry = new GuidEntry(activeEntry.entityName, activeEntry.guid, activeEntry.publicKey, activeEntry.privateKey);
			System.out.println(guidEntry);
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	private static ArrayList<String> loadDic(){
		ArrayList<String> dic = new ArrayList<String>();
		File file = new File("pl_ssh_stable");
		FileInputStream fis = null;
		StringBuilder builder = new StringBuilder();
		try{
			fis = new FileInputStream(file);
			int content;
			
			while((content = fis.read()) != -1){
				builder.append((char) content);
			}
			fis.close();
		}
		catch(IOException e){
			e.printStackTrace();
		}
		//System.out.print(builder);
		String[] list = builder.toString().split("\n");
		for(String host:list){
			if(!host.startsWith("#")){
				dic.add(host);
			}
		}
		return dic;
	}
	
	private static void updateForHost(String host, int server_num) throws Exception{
		String field = "host"+server_num;
		
		client.fieldCreateList(guidEntry.getGuid(), field, new JSONArray("[]"), guidEntry);
		
		File file = new File("/Users/zhaoyugao/Documents/planetlab/loc/"+host);
		FileInputStream fis = null;
		StringBuilder builder = new StringBuilder();
		try{
			fis = new FileInputStream(file);
			int content;
			
			while((content = fis.read()) != -1){
				builder.append((char) content);
			}
			fis.close();
		}
		catch(IOException e){
			e.printStackTrace();
		}
		
		ArrayList<ArrayList<Double>> lat = new ArrayList<ArrayList<Double>>();
		for (int i=0; i<num_server; i++){
			lat.add(new ArrayList<Double>());
		}
		
		String[] list = builder.toString().split("\n");
		int count = 0;
		for(String latency:list){
			lat.get(count).add(Double.parseDouble(latency));
			++count;
			count = count%num_server;
		}
		
		for (int i=0; i<num_server; i++){
			double total = 0;
			int cnt = 0; 
			for(double latency:lat.get(i)){
				if(latency != 0){
					total += latency;
				}else{
					total += 1000;
				}
				++cnt;
			}
			
			if(cnt!=0){
				double avg = total/cnt;
				
				JSONArray jsarr = new JSONArray("["+avg+"]");
				System.out.println(jsarr);
				client.fieldAppend(guidEntry.getGuid(), field, jsarr, guidEntry);
			}else{
				JSONArray jsarr = new JSONArray("[10000]");
				System.out.println(jsarr);
				client.fieldAppend(guidEntry.getGuid(), field, jsarr, guidEntry);
			}
			try{
				Thread.sleep(100);
			}catch(Exception e){}
		}
		
		JSONArray json = client.fieldReadArray(guidEntry.getGuid(), field, guidEntry);
		
		System.out.println(json);
	}
	
	/**
	 * Main
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException{
		Client();
		
		ArrayList<String> hosts = loadDic();
				
		for(int i=0; i<70; i++){
			try{
				client.fieldClear(guidEntry.getGuid(), "host"+i, guidEntry);
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		try{
			client.fieldCreateList(guidEntry.getGuid(), "load", new JSONArray("[0]"), guidEntry);
			for(int i=0; i<5; i++){
				client.fieldAppend(guidEntry.getGuid(), "load", new JSONArray("[0]"), guidEntry);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		
		int num = 0;
		for(String host:hosts){
			System.out.println(host);
			try{
				updateForHost(host, num);
			}catch(Exception e){
				e.printStackTrace();
			}
			num++;
		}
				
		System.out.println("done!");
		System.exit(0);
	}
}