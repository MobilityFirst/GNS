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

import com.google.common.net.InetAddresses;

import edu.umass.cs.gigapaxos.interfaces.Request;

import java.util.ArrayList;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsserver.utils.Util;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import edu.umass.cs.reconfiguration.reconfigurationutils.InterfaceGetActiveIPs;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

/**
 * This class maintains the demand profile for a single name.
 * It will select active name servers based on location of the demand.
 * It is based on the old LocationBasedReplication class, but doesn't
 * include checks for unreachable active servers or highly loaded servers.
 *
 *
 * @author Westy
 */
public class LocationBasedDemandProfile extends AbstractDemandProfile {

  private static final Logger LOG = Logger.getLogger(LocationBasedDemandProfile.class.getName());

  /**
   * The keys for the demand profile packet.
   */
  public enum Keys {

    
    /** SERVICE_NAME */ 
    SERVICE_NAME, 
    /** STATS */ 
    STATS, 
    /** RATE */ 
    RATE, 
    /** NUM_REQUESTS */ 
    NUM_REQUESTS,
    /** NUM_TOTAL_REQUESTS */ 
    NUM_TOTAL_REQUESTS, 
    /** VOTES_MAP */ 
    VOTES_MAP, 
    /** LOOKUP_COUNT */ 
    LOOKUP_COUNT, 
    /** UPDATE_COUNT */ 
    UPDATE_COUNT
  };

  /**
   * Only report this often.
   */
  private static final int NUMBER_OF_REQUESTS_BETWEEN_REPORTS = 100;
  /**
   * Don't reconfigure more often than this time interval. Both of these need to be satisfied.
   */
  private static final long MIN_RECONFIGURATION_INTERVAL = 60000; // milleseconds
  /**
   * Don't reconfigure more often than this many requests. Both of these need to be satisfied.
   */
  private static final long NUMBER_OF_REQUESTS_BETWEEN_RECONFIGURATIONS = 1000;

  private double interArrivalTime = 0.0;
  private long lastRequestTime = 0;
  private int numRequests = 0;
  private int numTotalRequests = 0;
  private LocationBasedDemandProfile lastReconfiguredProfile = null;
  private VotesMap votesMap = new VotesMap();
  private int lookupCount = 0;
  private int updateCount = 0;
  
  private boolean debuggingEnabled = false;

  /**
   * Create a LocationBasedDemandProfile instance.
   * 
   * @param name 
   */
  public LocationBasedDemandProfile(String name) {
    super(name);
  }

  /**
   * Create a LocationBasedDemandProfile instance by making a deep copy of another instance.
   * 
   * @param dp 
   */
  public LocationBasedDemandProfile(LocationBasedDemandProfile dp) {
    super(dp.name);
    this.interArrivalTime = dp.interArrivalTime;
    this.lastRequestTime = dp.lastRequestTime;
    this.numRequests = dp.numRequests;
    this.numTotalRequests = dp.numTotalRequests;
    this.votesMap = new VotesMap(dp.votesMap);
    this.lookupCount = dp.lookupCount;
    this.updateCount = dp.updateCount;
  }

  /**
   * Create a LocationBasedDemandProfile instance from a JSON packet. 
   * 
   * @param json
   * @throws org.json.JSONException
   */
  public LocationBasedDemandProfile(JSONObject json) throws JSONException {
    super(json.getString(Keys.SERVICE_NAME.toString()));
    this.interArrivalTime = 1.0 / json.getDouble(Keys.RATE.toString());
    this.numRequests = json.getInt(Keys.NUM_REQUESTS.toString());
    this.numTotalRequests = json.getInt(Keys.NUM_TOTAL_REQUESTS.toString());
    this.votesMap = new VotesMap(json.getJSONObject(Keys.VOTES_MAP.toString()));
    this.lookupCount = json.getInt(Keys.LOOKUP_COUNT.toString());
    this.updateCount = json.getInt(Keys.UPDATE_COUNT.toString());
    if (debuggingEnabled) {
      LOG.info("%%%%%%%%%%%%%%%%%%%%%%%%%>>> " + this.name + " VOTES MAP AFTER READ: " + this.votesMap);
    }
  }

  @Override
  public JSONObject getStats() {
    if (debuggingEnabled) {
      LOG.info("%%%%%%%%%%%%%%%%%%%%%%%%%>>> " + this.name + " VOTES MAP BEFORE GET STATS: " + this.votesMap);
    }
    JSONObject json = new JSONObject();
    try {
      json.put(Keys.SERVICE_NAME.toString(), this.name);
      json.put(Keys.RATE.toString(), getRequestRate());
      json.put(Keys.NUM_REQUESTS.toString(), getNumRequests());
      json.put(Keys.NUM_TOTAL_REQUESTS.toString(), getNumTotalRequests());
      json.put(Keys.VOTES_MAP.toString(), getVotesMap().toJSONObject());
      json.put(Keys.LOOKUP_COUNT.toString(), this.lookupCount);
      json.put(Keys.UPDATE_COUNT.toString(), this.updateCount);
    } catch (JSONException je) {
      je.printStackTrace();
    }
    if (debuggingEnabled) {
      LOG.info("%%%%%%%%%%%%%%%%%%%%%%%%%>>> " + this.name + " GET STATS: " + json);
    }
    return json;
  }

  /**
   * Create an empty LocationBasedDemandProfile instance for a name. 
   * @param name
   * @return 
   */
  public static LocationBasedDemandProfile createDemandProfile(String name) {
    return new LocationBasedDemandProfile(name);
  }

  @Override
  public void register(Request request, InetAddress sender, InterfaceGetActiveIPs nodeConfig) {
    if (!request.getServiceName().equals(this.name)) {
      return;
    }
    // This happens when called from a reconfigurator
    if (nodeConfig == null) {
      return;
    }
    this.numRequests++;
    this.numTotalRequests++;
    long iaTime = 0;
    if (lastRequestTime > 0) {
      iaTime = System.currentTimeMillis() - this.lastRequestTime;
      this.interArrivalTime = Util.movingAverage(iaTime, interArrivalTime);
    } else {
      lastRequestTime = System.currentTimeMillis(); // initialization
    }

    if (sender != null) { // should not happen, but just in case
      this.votesMap.increment(findActiveReplicaClosestToSender(sender, nodeConfig.getActiveIPs()));
    }

    if (request instanceof ReplicableRequest
            && ((ReplicableRequest) request).needsCoordination()) {
      updateCount++;
    } else {
      lookupCount++;
    }
    if (debuggingEnabled) {
      LOG.info("%%%%%%%%%%%%%%%%%%%%%%%%%>>> AFTER REGISTER:" + this.toString());
    }
  }

  private InetAddress findActiveReplicaClosestToSender(InetAddress sender, List<InetAddress> allActives) {
    assert !allActives.isEmpty();
    InetAddress closest = allActives.get(0);
    int minDistance = Integer.MAX_VALUE;
    for (InetAddress active : allActives.subList(1, allActives.size())) {
      int distance = distanceBetween(sender, active);
      if (distance < minDistance) {
        closest = active;
        minDistance = distance;
      }
    }
    return closest;
  }

  private int distanceBetween(InetAddress one, InetAddress two) {
    int result;
    try {
      // Probably a stupid matcher but it gets close.
      result = InetAddresses.coerceToInteger(one) ^ InetAddresses.coerceToInteger(two);
    } catch (Exception e) {
      result = Integer.MAX_VALUE;
    }
    //if (debuggingEnabled) {
    //LOG.finest("distance " + one + " - " + two + " = " + result);
    //}

    return result;
  }

  @Override
  public void reset() {
    this.interArrivalTime = 0.0;
    this.lastRequestTime = 0;
    this.numRequests = 0;
    this.votesMap = new VotesMap();
    this.updateCount = 0;
    this.lookupCount = 0;
  }

  @Override
  public LocationBasedDemandProfile clone() {
    return new LocationBasedDemandProfile(this);
  }

  @Override
  public void combine(AbstractDemandProfile dp) {
    LocationBasedDemandProfile update = (LocationBasedDemandProfile) dp;
    this.lastRequestTime = Math.max(this.lastRequestTime,
            update.lastRequestTime);
    this.interArrivalTime = Util.movingAverage(update.interArrivalTime,
            this.interArrivalTime, update.getNumRequests());
    this.numRequests += update.numRequests; // this number is not meaningful at RC
    this.numTotalRequests += update.numTotalRequests;
    this.updateCount += update.updateCount;
    this.lookupCount += update.lookupCount;
    this.votesMap.combine(update.getVotesMap());
    if (debuggingEnabled) {
      LOG.info("%%%%%%%%%%%%%%%%%%%%%%%%%>>> AFTER COMBINE:" + this.toString());
    }
  }

  @Override
  public boolean shouldReport() {
    if (getNumRequests() >= NUMBER_OF_REQUESTS_BETWEEN_REPORTS) {
      return true;
    }
    return false;
  }

  @Override
  public void justReconfigured() {
    this.lastReconfiguredProfile = this.clone();
    if (debuggingEnabled) {
      LOG.info("%%%%%%%%%%%%%%%%%%%%%%%%%>>> AFTER CLONE:" + this.lastReconfiguredProfile.toString());
    }
  }

  @Override
  public ArrayList<InetAddress> shouldReconfigure(ArrayList<InetAddress> curActives, InterfaceGetActiveIPs nodeConfig) {
    // This happens when called from a reconfigurator
    if (nodeConfig == null) {
      return null;
    }
    if (this.lastReconfiguredProfile != null) {
      if (debuggingEnabled) {
        LOG.info("%%%%%%%%%%%%%%%%%%%%%%%%%>>> LAST: " + this.lastReconfiguredProfile.toString());
        LOG.info("%%%%%%%%%%%%%%%%%%%%%%%%%>>> CURRENT: " + this.toString());
        LOG.info("%%%%%%%%%%%%%%%%%%%%%%%%%>>> interval: "
                + (System.currentTimeMillis() - this.lastReconfiguredProfile.lastRequestTime));
      }
      if (System.currentTimeMillis()
              - this.lastReconfiguredProfile.lastRequestTime < MIN_RECONFIGURATION_INTERVAL) {
        return null;
      }
      if (debuggingEnabled) {
        LOG.info("%%%%%%%%%%%%%%%%%%%%%%%%%>>> request diff: "
                + (this.numTotalRequests - this.lastReconfiguredProfile.numTotalRequests));
      }
      if (this.numTotalRequests
              - this.lastReconfiguredProfile.numTotalRequests < NUMBER_OF_REQUESTS_BETWEEN_RECONFIGURATIONS) {
        return null;
      }
    }
    int numberOfReplicas = computeNumberOfReplicas(lookupCount, updateCount, nodeConfig.getActiveIPs().size());
    if (debuggingEnabled) {
      LOG.info("%%%%%%%%%%%%%%%%%%%%%%%%%>>> " + this.name + " VOTES MAP: " + this.votesMap
              + " TOP: " + this.votesMap.getTopN(numberOfReplicas) + " Lookup: " + lookupCount
              + " Update: " + updateCount + " ReplicaCount: " + numberOfReplicas);
    }

    return pickNewActiveReplicas(numberOfReplicas, curActives,
            this.votesMap.getTopN(numberOfReplicas),
            nodeConfig.getActiveIPs());
  }

  //
  // Routines below for computing new actives list
  //
  /**
   * Returns a list of new active replicas based on read / write load.
   * Returns InetSocketAddresses based on the number needed (which is calculated in computeNumberOfReplicas
   * using read / write load), the current ones and the entire list available from nodeCOnfig.
   *
   * @param numReplica
   * @param curActives
   * @param nodeConfig
   * @return a list of InetSocketAddress
   */
  // NEED TO PICK A FRACTIONAL AMOUNT FOR LOCALITY-BASED ONES
  private ArrayList<InetAddress> pickNewActiveReplicas(int numReplica, ArrayList<InetAddress> curActives,
          ArrayList<InetAddress> topN, ArrayList<InetAddress> allActives) {

    // If we need more replicas than we have just return them all
    if (numReplica >= allActives.size()) {
      if (debuggingEnabled) {
        LOG.info("%%%%%%%%%%%%%%%%%%%%%%%%%>>> RETURNING ALL");
      }
      return allActives;
    }

    // Otherwise get the topN from the votes list
    ArrayList<InetAddress> newActives = new ArrayList<InetAddress>(topN);
    if (debuggingEnabled) {
      LOG.info("%%%%%%%%%%%%%%%%%%%%%%%%%>>> TOP N: " + newActives);
    }

    // If we have too many in top N then remove some from the end and return this list.
    // BTW: This is the only case that could maybe use some work because it 
    // totally ignores the current active set.
    if (numReplica < newActives.size()) {
      return new ArrayList<InetAddress>(newActives.subList(0, numReplica));
    }

    // Otherwise we need more than is contained in the top n.
    // Tack on some extras starting with current actives.
    if (newActives.size() < numReplica) {
      for (InetAddress current : curActives) {
        if (newActives.size() >= numReplica) {
          break;
        }
        if (!newActives.contains(current)) {
          newActives.add(current);
        }
      }
      if (debuggingEnabled) {
        LOG.info("%%%%%%%%%%%%%%%%%%%%%%%%%>>> WITH CURRENT ADDED: " + newActives);
      }
    }

    // If we still need more add some random ones from the active replicas list
    if (newActives.size() < numReplica) {
      // Note here that since for testing (but not operationally) we currently 
      // allow multiple actives to reside on the same
      // host differentiated by ports this loop might get the same ip multiple times. Nothing bad will
      // happen because of the contains below, but still it's kind of dumb.
      for (InetAddress ip : (ArrayList<InetAddress>) allActives) {
        if (newActives.size() >= numReplica) {
          break;
        }
        if (!newActives.contains(ip)) {
          newActives.add(ip);
        }
      }
      if (debuggingEnabled) {
        LOG.info("%%%%%%%%%%%%%%%%%%%%%%%%%>>> WITH RANDOM ADDED: " + newActives);
      }
    }

    return newActives;
  }

  /**
   * Returns the size of active replica set that should exist for this name record.
   * Depends on the read and update rate of this name record.
   * There are two special cases:
   * (1) if there are no lookups or updates for this name, it returns 0.
   * (2) if (numberReplicaControllers == 1), then the system is un-replicated, therefore it always returns 1;
   *
   * Otherwise returns a value in the range {@link edu.umass.cs.utils.Config.Config#minReplica} and
   * {@link edu.umass.cs.utils.Config.Config#maxReplica}.
   */
  private int computeNumberOfReplicas(int lookupCount, int updateCount, int actualReplicasCount) {
//    if (Config.singleNS) {
//      return 1; // this seems straightforward
//    } else 
    if (updateCount == 0) {
      // no updates, replicate everywhere.
      return Math.min(actualReplicasCount, AppReconfigurableNodeOptions.maxReplica);
    } else {
      // Can't be bigger than the number of actual replicas or the max configured amount
      return Math.min(Math.min(actualReplicasCount, AppReconfigurableNodeOptions.maxReplica),
              // Or smaller than the min configured amount
              Math.max(AppReconfigurableNodeOptions.minReplica,
                      (int) StrictMath.round(((double) lookupCount
                              / ((double) updateCount * AppReconfigurableNodeOptions.normalizingConstant)))));
    }
  }

  /**
   * Returns the request rate.
   * 
   * @return the request rate
   */
  public double getRequestRate() {
    return this.interArrivalTime > 0 ? 1.0 / this.interArrivalTime
            : 1.0 / (this.interArrivalTime + 1000);
  }

  /**
   * Return the number of requests.
   * 
   * @return the number of requests
   */
  public double getNumRequests() {
    return this.numRequests;
  }

  /**
   * Return the total number of requests.
   * 
   * @return the total number of requests
   */
  public double getNumTotalRequests() {
    return this.numTotalRequests;
  }
/**
 * Return the votes map.
 * 
 * @return the votes map
 */
  public VotesMap getVotesMap() {
    return votesMap;
  }

  @Override
  public String toString() {
    return "LocationBasedDemandProfile{" + "interArrivalTime=" + interArrivalTime + ", lastRequestTime=" + lastRequestTime + ", numRequests=" + numRequests + ", numTotalRequests=" + numTotalRequests + ", lastReconfiguredProfile=" + lastReconfiguredProfile + ", votesMap=" + votesMap + ", lookupCount=" + lookupCount + ", updateCount=" + updateCount + '}';
  }

  /**
   * JUST FOR TESTING!
   *
   */
  public LocationBasedDemandProfile() {
    super("FRANK");
    this.interArrivalTime = 2.0;
    this.lastRequestTime = System.currentTimeMillis();
    this.numRequests = 100;
    this.numTotalRequests = 200;
    this.votesMap = new VotesMap();
    this.lookupCount = 40;
    this.updateCount = 50;
  }

  /**
   * Main routine. Only for testing.
   * 
   * @param args
   * @throws UnknownHostException 
   */
  public static void main(String[] args) throws UnknownHostException {
    LocationBasedDemandProfile dp = new LocationBasedDemandProfile();
    LOG.info(dp.toString());
    LOG.info(dp.clone().toString());
    //testThings(dp);
  }

  private static void testThings(LocationBasedDemandProfile dp) throws UnknownHostException {
    LOG.info("0,0,3 = " + dp.computeNumberOfReplicas(0, 0, 3));
    LOG.info("20,0,3 = " + dp.computeNumberOfReplicas(20, 0, 3));
    LOG.info("0,20,3 = " + dp.computeNumberOfReplicas(0, 20, 3));
    LOG.info("20,20,3 = " + dp.computeNumberOfReplicas(20, 20, 3));
    LOG.info("2000,100,3 = " + dp.computeNumberOfReplicas(2000, 100, 3));
    LOG.info("100,200,3 = " + dp.computeNumberOfReplicas(100, 2000, 3));
    LOG.info("0,0,200 = " + dp.computeNumberOfReplicas(0, 0, 200));
    LOG.info("20,0,200 = " + dp.computeNumberOfReplicas(20, 0, 200));
    LOG.info("0,20,200 = " + dp.computeNumberOfReplicas(0, 20, 200));
    LOG.info("20,20,200 = " + dp.computeNumberOfReplicas(20, 20, 200));
    LOG.info("2000,100,200 = " + dp.computeNumberOfReplicas(2000, 100, 200));
    LOG.info("100,200,200 = " + dp.computeNumberOfReplicas(100, 2000, 200));
    LOG.info("10000,200,200 = " + dp.computeNumberOfReplicas(10000, 200, 200));

    // All the actives
    ArrayList<InetAddress> allActives = new ArrayList<>(Arrays.asList(
            InetAddress.getByName("128.119.1.1"),
            InetAddress.getByName("128.119.1.2"),
            InetAddress.getByName("128.119.1.3"),
            InetAddress.getByName("128.119.1.4"),
            InetAddress.getByName("128.119.1.5"),
            InetAddress.getByName("128.119.1.6"),
            InetAddress.getByName("128.119.1.7"),
            InetAddress.getByName("128.119.1.8"),
            InetAddress.getByName("128.119.1.9"),
            InetAddress.getByName("128.119.1.10"),
            InetAddress.getByName("128.119.10.2")));

    // The list of current actives
    ArrayList<InetAddress> curActives = new ArrayList<>(Arrays.asList(
            InetAddress.getByName("128.119.1.1"),
            InetAddress.getByName("128.119.1.2"),
            InetAddress.getByName("128.119.1.3")));

    // Simulates the top N vote getters
    ArrayList<InetAddress> topN = new ArrayList<>(Arrays.asList(
            InetAddress.getByName("128.119.1.2"),
            InetAddress.getByName("128.119.1.3"),
            InetAddress.getByName("128.119.1.4"),
            InetAddress.getByName("128.119.1.5")));

    LOG.info("closest to 128.119.1.1 " + dp.findActiveReplicaClosestToSender(InetAddress.getByName("128.119.1.1"), allActives));
    LOG.info("closest to 128.119.10.1 " + dp.findActiveReplicaClosestToSender(InetAddress.getByName("128.119.10.1"), allActives));

    // Try it with various numbers of active replicas needed
    LOG.info("need 3: " + dp.pickNewActiveReplicas(3, curActives, topN, allActives));

    LOG.info("need 4: " + dp.pickNewActiveReplicas(4, curActives, topN, allActives));

    LOG.info("need 5: " + dp.pickNewActiveReplicas(5, curActives, topN, allActives));

    LOG.info("need 6: " + dp.pickNewActiveReplicas(6, curActives, topN, allActives));

    LOG.info("need 7: " + dp.pickNewActiveReplicas(7, curActives, topN, allActives));

  }
}
