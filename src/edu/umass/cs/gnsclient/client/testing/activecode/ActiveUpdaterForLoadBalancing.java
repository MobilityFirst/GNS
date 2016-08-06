package edu.umass.cs.gnsclient.client.testing.activecode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONArray;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.GuidEntry;

/**
 * @author gaozy
 *
 */
public class ActiveUpdaterForLoadBalancing implements Runnable {
	
	ServerSocket listener;
	static ExecutorService executor;
	static GNSClientCommands gnsClient;
	static GuidEntry guidEntry;
	
	
	ActiveUpdaterForLoadBalancing(String filename){
		try {
			listener = new ServerSocket(9090);
		} catch (IOException e) {
			e.printStackTrace();
		}
		executor = Executors.newFixedThreadPool(5);
		
		try {
			gnsClient = new GNSClientCommands();
			ObjectInputStream input = new ObjectInputStream(new FileInputStream(new File(filename)));
			guidEntry = new GuidEntry(input);
			input.close();
		} catch (IOException e) {		
			e.printStackTrace();
		}
		
	}
	
	@Override
	public void run() {
		
			try {
				while(true){
					Socket socket = listener.accept();
					executor.execute(new RequestHandler(socket));
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					listener.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		
		
	}
	
	static class UpdateHandler implements Runnable{
		int numReqs;
		int index;
				
		UpdateHandler(int index, int numReqs){
			this.index = index;
			this.numReqs = numReqs;
		}
		
		@Override
		public void run() {
			
			try { 
				 // This real cost is used to record the real cost and it will be updated every minute 
				 // by adding the last minute cost to it. 
				 
				JSONArray realCost = gnsClient.fieldReadArray(guidEntry.getGuid(), "REALCOST", guidEntry);
				
				int realCostOfField = Integer.parseInt(realCost.get(index).toString());
				realCostOfField += numReqs;
				
				System.out.println("The real number of requests is "+numReqs+", the total real cost is "+realCostOfField+
						", and the load is "+numReqs/60+" reqs/sec");
				
				gnsClient.fieldSetElement(guidEntry.getGuid(), "REALCOST", ""+realCostOfField, index, guidEntry);
				gnsClient.fieldSetElement(guidEntry.getGuid(), "COST", ""+realCostOfField,index, guidEntry);
				gnsClient.fieldSetElement(guidEntry.getGuid(), "LOAD", ""+numReqs/60, index, guidEntry);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}
		
	}
	
	static class RequestHandler implements Runnable{
		Socket socket;
		
		RequestHandler(Socket socket){
			this.socket = socket;
		}
		
		@Override
		public void run() {
			try {
				BufferedReader input =
						new BufferedReader(new InputStreamReader(socket.getInputStream()));
				
				String line;
				while((line = input.readLine()) != null){
					System.out.println("Received:"+line);
					
					String[] param = line.split(" ");
					int index = Integer.parseInt(param[0]);
					int numReqs = Integer.parseInt(param[1]);
					
					executor.execute(new UpdateHandler(index, numReqs));
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
		
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args){
		//
		String path_to_key = "/Users/gaozy/Documents/EC2/guid";
		new Thread(new ActiveUpdaterForLoadBalancing(path_to_key)).start();
		System.out.println("Server start running on port 9090 ...");
	}
	
}
