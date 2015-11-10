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
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp;

import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.utils.Util;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * A map between InetAddress OR InetSocketAddress and access counts.
 * Note: This supports both InetAddress and InetSocketAddress as keys,
 * but not in the same VotesMap.
 *
 * @author westy
 */
public class VotesMap {

  JSONObject storage;

  /**
   * Creates a new empty VotesMap.
   */
  public VotesMap() {
    this.storage = new JSONObject();
  }

  /**
   * Creates a new VotesMap by copying a VotesMap.
   * 
   * @param votesMap
   */
  public VotesMap(VotesMap votesMap) {
    this(votesMap.storage);
  }

  /**
   * Creates a new VotesMap from a JSON Object.
   * @param json
   */
  public VotesMap(JSONObject json) {
    this();
    Iterator<?> keyIter = json.keys();
    while (keyIter.hasNext()) {
      String key = (String) keyIter.next();
      try {
        storage.put(key, json.get(key));
      } catch (JSONException e) {
        GNS.getLogger().severe("Unable to parse JSON: " + e);
      }
    }
  }

  /**
   * Converts a VotesMap object into a JSONObject.
   *
   * @return a JSONObject
   */
  public JSONObject toJSONObject() {
    return storage;
  }

  /**
   * Increments the value corresponding to the sender InetAddress by 1.
   *
   * @param sender
   */
  public void increment(InetAddress sender) {
    if (storage == null) {
      throw new RuntimeException("STORAGE IS NULL");
    }
    if (sender != null) { //not sure why this would happen, but just in case
      try {
        storage.increment(sender.getHostAddress());
      } catch (JSONException e) {
        GNS.getLogger().severe("Unable to parse JSON: " + e);
      }
    }
  }

  /**
   * Increments the value corresponding to the sender InetAddress by 1.
   *
   * @param sender
   */
  public void increment(InetSocketAddress sender) {
    if (storage == null) {
      throw new RuntimeException("STORAGE IS NULL");
    }
    try {
      storage.increment(sender.getHostString() + ":" + sender.getPort());
    } catch (JSONException e) {
      GNS.getLogger().severe("Unable to parse JSON: " + e);
    }
  }

  /**
   * Returns the top N vote getting InetAddresses in the map.
   * Will return less if there are not N distinct entries.
   *
   * @param n
   * @return an ArrayList of the top n
   */
  public ArrayList<InetAddress> getTopN(int n) {
    ArrayList<InetAddress> result = new ArrayList<>();
    // convert the JSONObject into a Map and sort it by value decreasing
    Map<String, Integer> map = Util.sortByValueDecreasing(toMap(storage));
    int cnt = 0;
    for (Map.Entry<String, Integer> entry : map.entrySet()) {
      if (cnt >= n) {
        break;
      }
      try {
        result.add(InetAddress.getByName(entry.getKey()));
      } catch (UnknownHostException e) {
        GNS.getLogger().severe("Unable to parse InetAddress: " + e);
      }
      cnt++;
    }
    return result;
  }

  /**
   * Returns the top N vote getting InetSocketAddress in the map.
   * Will return less if there are not N distinct entries.
   *
   * @param n
   * @return an ArrayList of the top n
   */
  public ArrayList<InetSocketAddress> getTopNIPSocket(int n) {
    ArrayList<InetSocketAddress> result = new ArrayList<>();
    // convert the JSONObject into a Map and sort it by value decreasing
    Map<String, Integer> map = Util.sortByValueDecreasing(toMap(storage));
    int cnt = 0;
    for (Map.Entry<String, Integer> entry : map.entrySet()) {
      if (cnt >= n) {
        break;
      }
      try {
        result.add(parseIPSocketString(entry.getKey()));
      } catch (UnknownHostException e) {
        GNS.getLogger().severe("Unable to parse InetAddress: " + e);
      }
      cnt++;
    }
    return result;
  }

  /**
   * Parses a string of the form host:port into an InetSocketAddress.
   *
   * @param string
   * @return an InetSocketAddress
   * @throws UnknownHostException
   */
  private InetSocketAddress parseIPSocketString(String string) throws UnknownHostException {
    String tokens[] = string.split(":");
    if (tokens.length != 2) {
      throw new UnknownHostException("Can't parse IPSocketString" + string);
    }
    return new InetSocketAddress(InetAddress.getByName(tokens[0]), Integer.parseInt(tokens[1]));
  }

  /**
   * Adds the votes from update to the votes in this object.
   *
   * @param update
   */
  public void combine(VotesMap update) {
    Iterator<?> keyIter = update.storage.keys();
    while (keyIter.hasNext()) {
      String key = (String) keyIter.next();
      try {
        // optInt returns zero if the key doesn't exist
        storage.put(key, storage.optInt(key) + update.storage.getInt(key));
      } catch (JSONException e) {
        GNS.getLogger().severe("Unable to parse JSON: " + e);
      }
    }
  }

  @Override
  public String toString() {
    return toJSONObject().toString();
  }

  /**
   * Main routine. For testing only.
   * 
   * @param args
   * @throws JSONException
   * @throws UnknownHostException
   */
  public static void main(String[] args) throws JSONException, UnknownHostException {
    System.out.println("InetAddress");
    VotesMap votesMap1 = new VotesMap();
    VotesMap votesMap2 = new VotesMap();

    votesMap1.increment(InetAddress.getByName("127.0.0.1"));
    votesMap1.increment(InetAddress.getByName("127.0.0.1"));
    votesMap1.increment(InetAddress.getByName("128.119.16.3"));

    votesMap2.increment(InetAddress.getByName("10.0.1.2"));
    votesMap2.increment(InetAddress.getByName("128.119.16.3"));
    votesMap2.increment(InetAddress.getByName("127.0.0.1"));

    VotesMap votesMap3 = new VotesMap(votesMap2);

    System.out.println(votesMap1);
    System.out.println(votesMap2);
    votesMap1.combine(votesMap2);
    System.out.println(votesMap1);
    votesMap1.combine(votesMap3);
    System.out.println(votesMap1);
    System.out.println(votesMap1.getTopN(10));

    System.out.println("InetSocketAddress");
    VotesMap votesMap11 = new VotesMap();
    VotesMap votesMap12 = new VotesMap();

    votesMap11.increment(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 1000));
    votesMap11.increment(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 2000));
    votesMap11.increment(new InetSocketAddress(InetAddress.getByName("128.119.16.3"), 3000));

    votesMap12.increment(new InetSocketAddress(InetAddress.getByName("10.0.1.2"), 4000));
    votesMap12.increment(new InetSocketAddress(InetAddress.getByName("128.119.16.3"), 5000));
    votesMap12.increment(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 1000));
    votesMap12.increment(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 1000));

    VotesMap votesMap13 = new VotesMap(votesMap12);

    System.out.println(votesMap11);
    System.out.println(votesMap12);
    votesMap11.combine(votesMap12);
    System.out.println(votesMap11);
    votesMap11.combine(votesMap13);
    System.out.println(votesMap11);
    System.out.println(votesMap11.getTopNIPSocket(10));
  }

  /**
   * Converts a JSONObject with integer values into a map.
   *
   * @param json
   * @return a map
   */
  private static Map<String, Integer> toMap(JSONObject json) {
    Map<String, Integer> map = new HashMap<String, Integer>();
    try {
      @SuppressWarnings("unchecked")
      Iterator<String> nameItr = json.keys();
      while (nameItr.hasNext()) {
        String name = nameItr.next();
        map.put(name, json.getInt(name));
      }
    } catch (JSONException e) {
      GNS.getLogger().severe("Unable to parse JSON: " + e);
    }
    return new HashMap<String, Integer>(map);
  }
}
