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
package edu.umass.cs.gnsclient.client.deprecated;

import edu.umass.cs.gnscommon.packets.CommandPacket;
import android.os.AsyncTask;

/**
 * This class defines a AndroidNIOTask
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
@Deprecated
public class AndroidNIOTask extends AsyncTask<Object, Void, String>
{
  // Needed to pass back to caller.
  private long id;
  
  /**
   *
   * @return a command packet
   */
  public CommandPacket getCommandPacket() {
	  throw new RuntimeException("Unimplemented");
  }

  /**
   *
   * @return the id
   */
  public long getId() 
  {
    return id;
  }

  /**
   *
   * @param id
   */
  public void setId(long id) 
  {
    this.id = id;
  }
  
  /**
   *
   * @param args
   * @return the result of executing the command
   */
  @Override
  protected String doInBackground(Object... args)
  {
    throw new UnsupportedOperationException("Not yet implemented");
//    JSONMessenger<InetSocketAddress> tcpTransport = (JSONMessenger<InetSocketAddress>) args[0];
//    JSONObject command = (JSONObject) args[1];
//    int id = (Integer) args[2];
//    InetSocketAddress destAddr = (InetSocketAddress) args[3];
//    Object monitor = args[4];
//    ConcurrentMap<Integer, Long> queryTimeStamp = (ConcurrentMap<Integer, Long>) args[5];
//    ConcurrentMap<Integer, CommandResult> resultMap = (ConcurrentMap<Integer, CommandResult>) args[6];
//    int readTimeout = (Integer) args[7];
//
//    CommandPacket packet = new CommandPacket(id, null, -1, command);
//    queryTimeStamp.put(id, System.currentTimeMillis());
//    
//    try {
//      JSONObject json = packet.toJSONObject();
//      tcpTransport.send(new GenericMessagingTask<>(destAddr, json));
//    } catch (//IOException | 
//            JSONException e) {
//     Log.e("GNS", "Failed to send message " + e);
//    }
//    // now we wait until the correct packet comes back
//    try
//    {
//      GNSClientConfig.getLogger().fine("Waiting for query id: " + id);
//      synchronized (monitor)
//      {
//        long startTime = System.currentTimeMillis();
//        while (!resultMap.containsKey(id) && System.currentTimeMillis() - startTime < readTimeout)
//        {
//          monitor.wait(readTimeout);
//        }
//        if (System.currentTimeMillis() - startTime >= readTimeout)
//        {
//          GNSClientConfig.getLogger().fine("TIMEOUT (" + id + ") : " + command.toString());
//          return GNSCommandProtocol.BAD_RESPONSE + " " + GNSCommandProtocol.TIMEOUT;
//        }
//      }
//      GNSClientConfig.getLogger().fine("Query id response received: " + id);
//    }
//    catch (InterruptedException x)
//    {
//      GNSClientConfig.getLogger().severe("Wait for return packet was interrupted " + x);
//    }
//    CommandResult result = resultMap.get(id);
//    resultMap.remove(id);
//    long sentTime = queryTimeStamp.get(id); // instrumentation
//    queryTimeStamp.remove(id); // instrumentation
//    long rtt = result.getReceivedTime() - sentTime;
//    GNSClientConfig.getLogger().info(
//        "Command name: " + command.optString(GNSCommandProtocol.COMMANDNAME, "Unknown") + " "
//            + command.optString(GNSCommandProtocol.GUID, "") + " " + command.optString(GNSCommandProtocol.NAME, "") + " id: " + id
//            + " RTT: " + rtt + "ms" + " LNS RTT: " + result.getCCPRoundTripTime() + "ms" + " NS: "
//            + result.getResponder() + " LNS Counter:" + result.getRequestCnt());
//    return result.getResult();
  }

}
