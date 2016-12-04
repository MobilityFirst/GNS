package edu.umass.cs.gnsserver.activecode.prototype;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnsserver.activecode.ActiveCodeHandler;
import edu.umass.cs.gnsserver.activecode.prototype.unblocking.ActiveNonBlockingClient.Monitor;
import edu.umass.cs.gnsserver.interfaces.ActiveDBInterface;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;

/**
 * This class is used for executing the queries sent from
 * ActiveWorker and sending response back.
 * @author gaozy
 *
 */
public class ActiveQueryHandler {
	private static ActiveDBInterface app;
	private final ThreadPoolExecutor queryExecutor;
	private final int numThread = 100;
	
	/**
	 * Initialize a query handler
	 * @param app
	 */
	public ActiveQueryHandler(ActiveDBInterface app){
		ActiveQueryHandler.app = app;
		this.queryExecutor = new ThreadPoolExecutor(numThread, numThread, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		queryExecutor.prestartAllCoreThreads();		
	}
	
	/**
	 * This method handles the incoming requests from ActiveQuerier,
	 * the query could be a read or write request.
	 * 
	 * @param am the query to handle
	 * @param header 
	 * @return an ActiveMessage being sent back to worker as a response to the query
	 */
	public ActiveMessage handleQuery(ActiveMessage am, InternalRequestHeader header){
		ActiveCodeHandler.getLogger().log(ActiveCodeHandler.DEBUG_LEVEL, "################ {0} receives:{1} ", new Object[]{this, am} );
		ActiveMessage response;
		if(am.type == ActiveMessage.Type.READ_QUERY)
			response = handleReadQuery(am, header);
		else
			response = handleWriteQuery(am, header);
		ActiveCodeHandler.getLogger().log(ActiveCodeHandler.DEBUG_LEVEL, "################ {0} returns response to worker:{1}", new Object[]{this, response} );
		return response;
	}
	
	private static class ActiveQuerierTask implements Runnable{
		ActiveMessage am;
		InternalRequestHeader header;
		Monitor monitor;
		
		ActiveQuerierTask(ActiveMessage am, InternalRequestHeader header, Monitor monitor){
			this.am = am;
			this.header = header;
			this.monitor = monitor;
		}
		
		@Override
		public void run() {
			ActiveCodeHandler.getLogger().log(ActiveCodeHandler.DEBUG_LEVEL, "################ {0} receives:{1} ", new Object[]{this, am} );
			ActiveMessage response;
			if(am.type == ActiveMessage.Type.READ_QUERY){
				try {
					JSONObject result = app.read(header, am.getTargetGuid(), am.getField());
					if(result != null)
						response = new ActiveMessage(am.getId(), result.toString(), null);
					else
						response = new ActiveMessage(am.getId(), null, "Read failed");
				} catch (InternalRequestException | ClientException e) {
					response = new ActiveMessage(am.getId(), null, "Read failed");
				} 
						
			}else{
				try {
					app.write(header, am.getTargetGuid(), am.getField(), new JSONObject(am.getValue()) );
					response = new ActiveMessage(am.getId(), new JSONObject().toString(), null);
				} catch (InternalRequestException | ClientException | JSONException e) {
					response = new ActiveMessage(am.getId(), null, "Write failed");
				}
				
			}
			ActiveCodeHandler.getLogger().log(ActiveCodeHandler.DEBUG_LEVEL, "################ {0} returns response to worker:{1}", new Object[]{this, response} );
			
			monitor.setResult(response, false);
		}
		
	}
	
	/**
	 * Submit this task to a thread pool
	 * @param am
	 * @param header
	 * @param monitor
	 */
	public void handleQueryAsync(ActiveMessage am, InternalRequestHeader header, Monitor monitor){
		queryExecutor.execute(new ActiveQuerierTask(am, header, monitor));
				
	}
	
	/**
	 * This method handles read query from the worker. 
	 * 
	 * @param am 
	 * @param header 
	 * @return the response ActiveMessage
	 */
	public ActiveMessage handleReadQuery(ActiveMessage am, InternalRequestHeader header) {		
		ActiveMessage resp = null;
		try {
			JSONObject value = app.read(header, am.getTargetGuid(), am.getField());
			resp = new ActiveMessage(am.getId(), value.toString(), null);
		} catch (InternalRequestException | ClientException e) {
			resp = new ActiveMessage(am.getId(), null, "Read failed");
		} 
		
		return resp;
	}

	
	/**
	 * This method handles write query from the worker. 
	 * 
	 * @param am 
	 * @param header 
	 * @return the response ActiveMessage
	 */
	public ActiveMessage handleWriteQuery(ActiveMessage am, InternalRequestHeader header) {
		ActiveMessage resp;
		try {
			app.write(header, am.getTargetGuid(), am.getField(), new JSONObject(am.getValue()));
			resp = new ActiveMessage(am.getId(), new JSONObject().toString(), null);
		} catch (ClientException | InternalRequestException | JSONException e) {
			resp = new ActiveMessage(am.getId(), null, "Write failed");
		} 
				
		return resp;
	}
	
	
	  /**
	   * This class is used to send a http request
	   *
	   * @param url
	   * @return response as a string
	   */
	public String httpRequest(String url){
	  StringBuilder response = new StringBuilder();
	  BufferedReader br = null;
	  try{
		  URL obj = new URL(url);
		  HttpURLConnection con = (HttpURLConnection) obj.openConnection();
		  con.setRequestMethod("GET");
		  con.setRequestProperty("User-Agent", "Mozilla/5.0");
		  InputStream in = con.getInputStream();
		   
		  br = new BufferedReader(new InputStreamReader(in));
		  
		  String line = "";
		  
		  while ((line = br.readLine()) != null) {
				response.append(line);
		  }
		
	  }catch(IOException e){
		  e.printStackTrace();
	  }finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
	  }
	  
	  return response.toString();
	}
	
	public String toString(){
		return this.getClass().getSimpleName();
	}
	
	/**
	 * @param args
	 * @throws JSONException
	 * @throws ActiveException 
	 */
	public static void main(String[] args) throws JSONException, ActiveException{
		
		String guid = "zhaoyu gao";
		String field = "nextGuid";
		String depth_code = "";
		try {
			depth_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/chain.js")));
		} catch (IOException e) {
			e.printStackTrace();
		} 
		JSONObject value = new JSONObject();
		value.put("nextGuid", "alvin");
		
		ActiveHandler handler = new ActiveHandler("", null, 1);
		
		int n = 1000000;
		
		long t1 = System.currentTimeMillis();
		
		for(int i=0; i<n; i++){
			handler.runCode(null, guid, field, depth_code, value, 0);
		}
		
		
		long elapsed = System.currentTimeMillis() - t1;
		System.out.println("It takes "+elapsed+"ms, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");
	}
}
