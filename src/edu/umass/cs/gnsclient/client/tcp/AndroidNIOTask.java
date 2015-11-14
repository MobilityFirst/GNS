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
 *  Initial developer(s): Westy, Emmanuel Cecchet
 *
 */
package edu.umass.cs.gnsclient.client.tcp;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentMap;

import org.json.JSONObject;




import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnscommon.GnsProtocol;
import edu.umass.cs.nio.NIOTransport;
import edu.umass.cs.gnsclient.client.tcp.packet.CommandPacket;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.JSONMessenger;

import java.io.IOException;

import org.json.JSONException;

import android.os.AsyncTask;
import android.util.Log;

/**
 * This class defines a AndroidNIOTask
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class AndroidNIOTask extends AsyncTask<Object, Void, String>
{
  // Needed to pass back to caller.
  private int id;

  public int getId() 
  {
    return id;
  }

  public void setId(int id) 
  {
    this.id = id;
  }
  
  @SuppressWarnings("unchecked")
  @Override
  protected String doInBackground(Object... args)
  {
    JSONMessenger<InetSocketAddress> tcpTransport = (JSONMessenger<InetSocketAddress>) args[0];
    JSONObject command = (JSONObject) args[1];
    int id = (Integer) args[2];
    InetSocketAddress destAddr = (InetSocketAddress) args[3];
    Object monitor = args[4];
    ConcurrentMap<Integer, Long> queryTimeStamp = (ConcurrentMap<Integer, Long>) args[5];
    ConcurrentMap<Integer, CommandResult> resultMap = (ConcurrentMap<Integer, CommandResult>) args[6];
    int readTimeout = (Integer) args[7];

    CommandPacket packet = new CommandPacket(id, null, -1, command);
    //String headeredMsg = JSONMessageExtractor.prependHeader(packet.toString());
    queryTimeStamp.put(id, System.currentTimeMillis());
    
    try {
      JSONObject json = packet.toJSONObject();
      tcpTransport.send(new GenericMessagingTask<>(destAddr, json));
    } catch (IOException | JSONException e) {
     Log.e("GNS", "Failed to send message " + e);
    }
//    try
//    {
//      tcpTransport.send(destAddr, headeredMsg.getBytes());
//    }
//    catch (IOException e)
//    {
//      
//    }
    // now we wait until the correct packet comes back
    try
    {
      GNSClient.getLogger().fine("Waiting for query id: " + id);
      synchronized (monitor)
      {
        long startTime = System.currentTimeMillis();
        while (!resultMap.containsKey(id) && System.currentTimeMillis() - startTime < readTimeout)
        {
          monitor.wait(readTimeout);
        }
        if (System.currentTimeMillis() - startTime >= readTimeout)
        {
          GNSClient.getLogger().fine("TIMEOUT (" + id + ") : " + command.toString());
          return GnsProtocol.BAD_RESPONSE + " " + GnsProtocol.TIMEOUT;
        }
      }
      GNSClient.getLogger().fine("Query id response received: " + id);
    }
    catch (InterruptedException x)
    {
      GNSClient.getLogger().severe("Wait for return packet was interrupted " + x);
    }
    CommandResult result = resultMap.get(id);
    resultMap.remove(id);
    long sentTime = queryTimeStamp.get(id); // instrumentation
    queryTimeStamp.remove(id); // instrumentation
    long rtt = result.getReceivedTime() - sentTime;
    GNSClient.getLogger().info(
        "Command name: " + command.optString(GnsProtocol.COMMANDNAME, "Unknown") + " "
            + command.optString(GnsProtocol.GUID, "") + " " + command.optString(GnsProtocol.NAME, "") + " id: " + id
            + " RTT: " + rtt + "ms" + " LNS RTT: " + result.getCCPRoundTripTime() + "ms" + " NS: "
            + result.getResponder() + " LNS Counter:" + result.getRequestCnt());
    return result.getResult();
  }

}
