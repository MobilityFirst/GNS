/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 */
package edu.umass.cs.gnsserver.deprecated.nio;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/* This class is deprecated. The plan is to move to JSONMessageWorker instead. */
  @Deprecated
  @SuppressWarnings("unchecked")
  public class ByteStreamToJSONObjects implements Runnable {

  HashMap socketData = new HashMap();


  private List queue = new LinkedList();
  int recvdMessageCount = 0;

  AbstractJSONPacketDemultiplexer packetDemux;

  public ByteStreamToJSONObjects(AbstractJSONPacketDemultiplexer packetDemux) {
    this.packetDemux = packetDemux;
  }

  public AbstractJSONPacketDemultiplexer getPacketDemux() {
    return packetDemux;
  }

  /*
  public void processData2(NioServer server, SocketChannel socket, byte[] data, int count) {

    byte[] dataCopy = new byte[count];
    System.arraycopy(data, 0, dataCopy, 0, count);
    synchronized(queue) {
      queue.add(new ServerDataEvent(server, socket, dataCopy));
      queue.notify();
    }
  }*/
  public void processData(SocketChannel socket, byte[] data, int count) {
    String y = new String(data, 0, count);
    if (!socketData.containsKey(socket)) {
      socketData.put(socket, null);
    }

    String x = (String) socketData.get(socket);
    if (x == null || x.length() == 0) {
      x = y;
    } else {
      x = x + y;
    }
    ArrayList allJSONs = new ArrayList();
    String remainingString = getJSONs(x, 0, allJSONs);
    socketData.put(socket, remainingString);

    // call the packet demultiplexer object.
    for (Object jsonObject: allJSONs)
      packetDemux.handleMessage((JSONObject) jsonObject);

//        recvdMessageCount++;
//        if (StartNameServer.debugMode) GNS.getLogger().finer(y + " Received message count = "
//                + recvdMessageCount);

  }

  public void run() {
    ServerDataEvent dataEvent;

    while(true) {
      // Wait for data to become available
      synchronized(queue) {
        while(queue.isEmpty()) {
          try {
            queue.wait();
          } catch (InterruptedException e) {
          }
        }
        dataEvent = (ServerDataEvent) queue.remove(0);
        if (!socketData.containsKey(dataEvent.socket)) {
          socketData.put(dataEvent.socket, null);
        }

        String x = (String) socketData.get(dataEvent.socket);
        if (x == null || x.length() == 0) {
          x = new String(dataEvent.data);
        } else {
          x = x + new String(dataEvent.data);
        }
        ArrayList allJSONs = new ArrayList();
        String remainingString = getJSONs(x, 0, allJSONs);
        socketData.put(dataEvent.socket, remainingString);

        // call the packet demultiplexer object.
        for (Object jsonObject: allJSONs)
          packetDemux.handleMessage((JSONObject) jsonObject);


        recvdMessageCount++;
        GNS.getLogger().finer(new String(dataEvent.data) + " Received message count = "
                + recvdMessageCount);
      }

      // Return to sender
      //			dataEvent.server.send(dataEvent.socket, dataEvent.data);
    }
  }

  public static String getJSONs(String data, int startIndex, ArrayList allJSONs) {
    if (startIndex < 0) {
      GNS.getLogger().severe("Start Index cant be negative");
      System.exit(2);
    }
    if (data == null || data.length() == 0) {
      return data;
    }
    if (allJSONs == null) {
      GNS.getLogger().severe("Can't return parsed json string. allJSONs ArrayList is null.");
      System.exit(2);
    }

    // it will always start with '&'
    int end = data.indexOf('&', startIndex + 1);
    if (end == -1) {
      // packet length header not yet received
      if (startIndex > 0) {
        return data.substring(startIndex);
      }
      return data;
    }
    // get the length of packet
    int packetLength = Integer.parseInt(data.substring(startIndex + 1, end));
    if (data.length() >= end + 1 + packetLength) {
      try
      {
        JSONObject json;
        json = new JSONObject(data.substring(end + 1, end + 1 + packetLength));
        allJSONs.add(json);
      } catch (JSONException e)
      {
        GNS.getLogger().fine("JSON Exception here. Move on.");
        e.printStackTrace();
      }
      // recursive call
      return getJSONs(data, end + 1 + packetLength, allJSONs);
    }
    if (startIndex > 0) {
      return data.substring(startIndex);
    }
    return data;
  }

  public static void main(String[] args) {
    //
    String filePath = "/Users/abhigyan/Documents/workspace/GNRS-westy/jsons.txt";
    try {
      BufferedReader br = new BufferedReader(new FileReader(filePath));
      int count = 0;
      StringBuilder sb = new StringBuilder();
      while(br.ready()) {
        count++;
        String jsonString = br.readLine();
        sb.append("&" + jsonString.length() + "&" + jsonString);
      }
      br.close();
      System.out.println(sb.toString());
      ArrayList allJSONs = new ArrayList();
      ByteStreamToJSONObjects.getJSONs(sb.toString(), 0, allJSONs);
      for (Object j: allJSONs) {
        System.out.println("OBJECT:" + (JSONObject) j);
      }
    } catch (NumberFormatException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}
