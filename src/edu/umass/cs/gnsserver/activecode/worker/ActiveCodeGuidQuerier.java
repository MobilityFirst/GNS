/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Misha Badov, Westy
 *
 */
package edu.umass.cs.gnsserver.activecode.worker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.DatagramSocket;
import java.net.HttpURLConnection;
import java.net.URL;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsserver.activecode.ActiveCodeUtils;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeMessage;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeQueryRequest;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeQueryResponse;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * This class is used to send query to active code client
 * 
 * @author Zhaoyu Gao
 */
public class ActiveCodeGuidQuerier {

  private DatagramSocket socket;
  private int clientPort;
  private String guid = "";
  private int depth = 0;
  private String error = null;
  private byte[] buffer = new byte[1024];
  
  /**
   * Initialize an ActiveCodeGuidQuerier
 * @param socket 
 * @param clientPort 
   */
  public ActiveCodeGuidQuerier(DatagramSocket socket, int clientPort) {
    this.socket = socket;
    this.clientPort = clientPort;
  }
  
  
  /**
   * 
   * @param depth the left depth for this code's execution
   */
  protected void setParam(int depth, String guid){
	  this.depth = depth;
	  this.guid = guid;
  }
  
  protected void setError(String err){
	  this.error = err;
  }
  
  private synchronized boolean accounting(String obj_guid){
	  if(this.depth <= 0){
		  return false;
	  }
	  this.depth--;
	  
	  return true;
  }
  
  /**
   * Queries (read or write) a guid
   *
   * @param acqreq the request params
   * @return the response
   */
  private ActiveCodeQueryResponse queryGuid(ActiveCodeQueryRequest acqreq) {
    try {
      ActiveCodeMessage acm = new ActiveCodeMessage();
      acm.setAcqreq(acqreq);
      // Send off the query request
      ActiveCodeUtils.sendMessage(socket, acm, clientPort);
      ActiveCodeMessage acmResp = ActiveCodeUtils.receiveMessage(socket, buffer);
      ActiveCodeQueryResponse acqr = acmResp.getAcqresp();
      
      System.out.println(">>>>>>>>>>>>>>>> Received the message "+acqr);
      return acqr;

    } catch (Exception e) {
      e.printStackTrace();
    }
    // Return an empty response to designate failure
    return new ActiveCodeQueryResponse();
  }
  
  /**
   * Reads a guid by passing the query on to the GNS process
   *
   * @param guid the guid
   * @param field the field
   * @return the ValuesMap response
   */
  public ValuesMap readGuid(String guid, String field) {
	  if (!accounting(guid)){
		  error = "Out of read limitation";
		  return null;
	  }
	//System.out.println("GUID is "+guid+", field is "+field);
    ActiveCodeQueryRequest acqreq = new ActiveCodeQueryRequest(guid, field, null, "read", depth);
    ActiveCodeQueryResponse acqresp = queryGuid(acqreq);

    ValuesMap vm = null;

    try {
      vm = new ValuesMap(new JSONObject(acqresp.getValuesMapString()));
    } catch (JSONException e) {
      e.printStackTrace();
    }
    
    return vm;
  }

  /**
   * Writes to a guid by passing the query on to the GNS process
   * (Only local guid currently supported).
   *
   * @param guid the guid
   * @param field the field
   * @param newValue the new values as a object
   * @return whether or not the write succeeded
   */
  public boolean writeGuid(String guid, String field, Object newValue){
	  if (!accounting(guid)){
		  //throw new ActiveCodeException("Out of write limitation");
		  error = "Out of write limitation";
		  return false;
	  }
    try {
      if (!(newValue instanceof ValuesMap)) {
        ValuesMap valuesMap = new ValuesMap();
        valuesMap.put(field, newValue);
        newValue = valuesMap;
      }
      ActiveCodeQueryRequest acqreq = new ActiveCodeQueryRequest(guid, field, newValue.toString(), "write", depth);
      ActiveCodeQueryResponse acqresp = queryGuid(acqreq);
      return acqresp.isSuccess();
    } catch (JSONException e) {
      e.printStackTrace();
      return false;
    }
  }
  /**
   * This class is used to send a http request
   * @param url
   * @return response as a string
   */
  public String httpRequest(String url){
	  if (!accounting(guid)){
		  return null;
	  }
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
  
  protected String getError(){
	  return error;
  }
}
