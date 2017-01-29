
package edu.umass.cs.gnsserver.gnsapp;

import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.utils.Util;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import org.json.JSONException;
import org.json.JSONObject;


public class VotesMap {

  private JSONObject storage;


  public VotesMap() {
    this.storage = new JSONObject();
  }


  public VotesMap(VotesMap votesMap) {
    this(votesMap.storage);
  }


  public VotesMap(JSONObject json) {
    this();
    Iterator<?> keyIter = json.keys();
    while (keyIter.hasNext()) {
      String key = (String) keyIter.next();
      try {
        storage.put(key, json.get(key));
      } catch (JSONException e) {
        GNSConfig.getLogger().log(Level.SEVERE, "Unable to parse JSON: {0}", e);
      }
    }
  }


  public JSONObject toJSONObject() {
    return storage;
  }


  public void increment(InetAddress sender) {
    if (storage == null) {
      throw new RuntimeException("STORAGE IS NULL");
    }
    if (sender != null) { //not sure why this would happen, but just in case
      try {
        storage.increment(sender.getHostAddress());
      } catch (JSONException e) {
        GNSConfig.getLogger().log(Level.SEVERE, "Unable to parse JSON: {0}", e);
      }
    }
  }


  public void increment(InetSocketAddress sender) {
    if (storage == null) {
      throw new RuntimeException("STORAGE IS NULL");
    }
    try {
      storage.increment(sender.getHostString() + ":" + sender.getPort());
    } catch (JSONException e) {
      GNSConfig.getLogger().log(Level.SEVERE, "Unable to parse JSON: {0}", e);
    }
  }


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
        GNSConfig.getLogger().log(Level.SEVERE, "Unable to parse InetAddress: {0}", e);
      }
      cnt++;
    }
    return result;
  }


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
        GNSConfig.getLogger().log(Level.SEVERE, "Unable to parse InetAddress: {0}", e);
      }
      cnt++;
    }
    return result;
  }


  private InetSocketAddress parseIPSocketString(String string) throws UnknownHostException {
    String tokens[] = string.split(":");
    if (tokens.length != 2) {
      throw new UnknownHostException("Can't parse IPSocketString" + string);
    }
    return new InetSocketAddress(InetAddress.getByName(tokens[0]), Integer.parseInt(tokens[1]));
  }


  public void combine(VotesMap update) {
    Iterator<?> keyIter = update.storage.keys();
    while (keyIter.hasNext()) {
      String key = (String) keyIter.next();
      try {
        // optInt returns zero if the key doesn't exist
        storage.put(key, storage.optInt(key) + update.storage.getInt(key));
      } catch (JSONException e) {
        GNSConfig.getLogger().log(Level.SEVERE, "Unable to parse JSON: {0}", e);
      }
    }
  }

  @Override
  public String toString() {
    return toJSONObject().toString();
  }


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


  private static Map<String, Integer> toMap(JSONObject json) {
    Map<String, Integer> map = new HashMap<>();
    try {
      @SuppressWarnings("unchecked")
      Iterator<String> nameItr = json.keys();
      while (nameItr.hasNext()) {
        String name = nameItr.next();
        map.put(name, json.getInt(name));
      }
    } catch (JSONException e) {
      GNSConfig.getLogger().log(Level.SEVERE, "Unable to parse JSON: {0}", e);
    }
    return new HashMap<>(map);
  }
}
