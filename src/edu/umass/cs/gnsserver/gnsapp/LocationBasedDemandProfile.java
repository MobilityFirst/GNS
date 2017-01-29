
package edu.umass.cs.gnsserver.gnsapp;

import com.google.common.net.InetAddresses;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnsserver.utils.Util;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import edu.umass.cs.reconfiguration.reconfigurationutils.InterfaceGetActiveIPs;
import edu.umass.cs.utils.DefaultTest;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


public class LocationBasedDemandProfile extends AbstractDemandProfile {

  private static final Logger LOG = Logger.getLogger(LocationBasedDemandProfile.class.getName());
  //FIXME: Do this have an equivalent in gigapaxos we can use.

  private static int maxReplica = 100;
  //FIXME: Do this have an equivalent in gigapaxos we can use.

  private static double normalizingConstant = 0.5;
  //FIXME: Do this have an equivalent in gigapaxos we can use.

  private static int minReplica = 3;


  private enum Keys {


    SERVICE_NAME,

    STATS,

    RATE,

    NUM_REQUESTS,

    NUM_TOTAL_REQUESTS,

    VOTES_MAP,

    LOOKUP_COUNT,

    UPDATE_COUNT
  };


  private static final int NUMBER_OF_REQUESTS_BETWEEN_REPORTS = 100;

  private static final long MIN_RECONFIGURATION_INTERVAL = 60000; // milleseconds

  private static final long NUMBER_OF_REQUESTS_BETWEEN_RECONFIGURATIONS = 1000;

  private double interArrivalTime = 0.0;
  private long lastRequestTime = 0;
  private int numRequests = 0;
  private int numTotalRequests = 0;
  private LocationBasedDemandProfile lastReconfiguredProfile = null;
  private VotesMap votesMap = new VotesMap();
  private int lookupCount = 0;
  private int updateCount = 0;


  public LocationBasedDemandProfile(String name) {
    super(name);
  }


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


  public LocationBasedDemandProfile(JSONObject json) throws JSONException {
    super(json.getString(Keys.SERVICE_NAME.toString()));
    this.interArrivalTime = 1.0 / json.getDouble(Keys.RATE.toString());
    this.numRequests = json.getInt(Keys.NUM_REQUESTS.toString());
    this.numTotalRequests = json.getInt(Keys.NUM_TOTAL_REQUESTS.toString());
    this.votesMap = new VotesMap(json.getJSONObject(Keys.VOTES_MAP.toString()));
    this.lookupCount = json.getInt(Keys.LOOKUP_COUNT.toString());
    this.updateCount = json.getInt(Keys.UPDATE_COUNT.toString());
    LOG.log(Level.FINE, "%%%%%%%%%%%%%%%%%%%%%%%%%>>> {0} VOTES MAP AFTER READ: {1}", new Object[]{this.name, this.votesMap});
  }


  @Override
  public JSONObject getStats() {
    LOG.log(Level.FINE, "%%%%%%%%%%%%%%%%%%%%%%%%%>>> {0} VOTES MAP BEFORE GET STATS: {1}", new Object[]{this.name, this.votesMap});
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
    LOG.log(Level.FINE, "%%%%%%%%%%%%%%%%%%%%%%%%%>>> {0} GET STATS: {1}", new Object[]{this.name, json});
    return json;
  }


  public static LocationBasedDemandProfile createDemandProfile(String name) {
    return new LocationBasedDemandProfile(name);
  }


  private static boolean shouldIgnore(Request request) {
    if (!(request instanceof CommandPacket)) {
      return true;
    }
    // else
    CommandPacket command = (CommandPacket) request;
    return command.getCommandType().isCreateDelete()
            || command.getCommandType().isSelect();
  }


  @Override
  public void register(Request request, InetAddress sender, InterfaceGetActiveIPs nodeConfig) {
    if (!request.getServiceName().equals(this.name)) {
      return;
    }

    if (shouldIgnore(request)) {
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
    LOG.log(Level.FINE, "%%%%%%%%%%%%%%%%%%%%%%%%%>>> AFTER REGISTER:{0}", this.toString());
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

  // TODO: arun: should use better IP-to-geo techniques here.
  private int distanceBetween(InetAddress one, InetAddress two) {
    int result;
    try {
      // Probably a stupid matcher but it gets close.
      result = InetAddresses.coerceToInteger(one)
              ^ InetAddresses.coerceToInteger(two);
    } catch (Exception e) {
      result = Integer.MAX_VALUE;
    }
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
    LOG.log(Level.FINE, "%%%%%%%%%%%%%%%%%%%%%%%%%>>> AFTER COMBINE:{0}", this.toString());
  }


  @Override
  public boolean shouldReport() {
    return getNumRequests() >= NUMBER_OF_REQUESTS_BETWEEN_REPORTS;
  }


  @Override
  public void justReconfigured() {
    this.lastReconfiguredProfile = this.clone();
    LOG.log(Level.FINE, "%%%%%%%%%%%%%%%%%%%%%%%%%>>> AFTER CLONE:{0}",
            this.lastReconfiguredProfile.toString());
  }


  @Override
  public ArrayList<InetAddress> shouldReconfigure(ArrayList<InetAddress> curActives, InterfaceGetActiveIPs nodeConfig) {
    // This happens when called from a reconfigurator
    if (nodeConfig == null) {
      return null;
    }
    if (this.lastReconfiguredProfile != null) {
      LOG.log(Level.FINE, "%%%%%%%%%%%%%%%%%%%%%%%%%>>> LAST: {0}", this.lastReconfiguredProfile.toString());
      LOG.log(Level.FINE, "%%%%%%%%%%%%%%%%%%%%%%%%%>>> CURRENT: {0}", this.toString());
      LOG.log(Level.FINE, "%%%%%%%%%%%%%%%%%%%%%%%%%>>> interval: {0}",
              (System.currentTimeMillis() - this.lastReconfiguredProfile.lastRequestTime));
      if (System.currentTimeMillis()
              - this.lastReconfiguredProfile.lastRequestTime < MIN_RECONFIGURATION_INTERVAL) {
        return null;
      }
      LOG.log(Level.FINE, "%%%%%%%%%%%%%%%%%%%%%%%%%>>> request diff: {0}",
              (this.numTotalRequests - this.lastReconfiguredProfile.numTotalRequests));

      if (this.numTotalRequests
              - this.lastReconfiguredProfile.numTotalRequests < NUMBER_OF_REQUESTS_BETWEEN_RECONFIGURATIONS) {
        return null;
      }
    }
    int numberOfReplicas = computeNumberOfReplicas(lookupCount, updateCount, nodeConfig.getActiveIPs().size());
    LOG.log(Level.INFO, "%%%%%%%%%%%%%%%%%%%%%%%%%>>> {0} "
            + "VOTES MAP: {1} TOP: {2} Lookup: {3} Update: {4} ReplicaCount: {5}",
            new Object[]{this.name, this.votesMap, this.votesMap.getTopN(numberOfReplicas),
              lookupCount, updateCount, numberOfReplicas});

    return pickNewActiveReplicas(numberOfReplicas, curActives,
            this.votesMap.getTopN(numberOfReplicas),
            nodeConfig.getActiveIPs());
  }

  // Routines below for computing new actives list

  // NEED TO PICK A FRACTIONAL AMOUNT FOR LOCALITY-BASED ONES
  private ArrayList<InetAddress> pickNewActiveReplicas(int numReplica, ArrayList<InetAddress> curActives,
          ArrayList<InetAddress> topN, ArrayList<InetAddress> allActives) {

    // If we need more replicas than we have just return them all
    if (numReplica >= allActives.size()) {
      LOG.fine("%%%%%%%%%%%%%%%%%%%%%%%%%>>> RETURNING ALL");
      return allActives;
    }

    // Otherwise get the topN from the votes list
    ArrayList<InetAddress> newActives = new ArrayList<>(topN);
    LOG.log(Level.FINE, "%%%%%%%%%%%%%%%%%%%%%%%%%>>> TOP N: {0}", newActives);

    // If we have too many in top N then remove some from the end and return this list.
    // BTW: This is the only case that could maybe use some work because it 
    // totally ignores the current active set.
    if (numReplica < newActives.size()) {
      return new ArrayList<>(newActives.subList(0, numReplica));
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
      LOG.log(Level.FINE, "%%%%%%%%%%%%%%%%%%%%%%%%%>>> WITH CURRENT ADDED: {0}", newActives);
    }

    // If we still need more add some random ones from the active replicas list
    if (newActives.size() < numReplica) {
      // Note here that since for testing (but not operationally) we currently 
      // allow multiple actives to reside on the same
      // host differentiated by ports this loop might get the same ip multiple times. Nothing bad will
      // happen because of the contains below, but still it's kind of dumb.
      for (InetAddress ip : allActives) {
        if (newActives.size() >= numReplica) {
          break;
        }
        if (!newActives.contains(ip)) {
          newActives.add(ip);
        }
      }
      LOG.log(Level.FINE, "%%%%%%%%%%%%%%%%%%%%%%%%%>>> WITH RANDOM ADDED: {0}", newActives);
    }

    return newActives;
  }


  private int computeNumberOfReplicas(int lookupCount, int updateCount, int actualReplicasCount) {

    if (updateCount == 0) {
      // no updates, replicate everywhere.
      return Math.min(actualReplicasCount, maxReplica);
    } else {
      // Can't be bigger than the number of actual replicas or the max configured amount
      return Math.min(Math.min(actualReplicasCount, maxReplica),
              // Or smaller than the min configured amount
              Math.max(minReplica,
                      (int) StrictMath.round((lookupCount
                              / (updateCount * normalizingConstant)))));
    }
  }


  public double getRequestRate() {
    return this.interArrivalTime > 0 ? 1.0 / this.interArrivalTime
            : 1.0 / (this.interArrivalTime + 1000);
  }


  public double getNumRequests() {
    return this.numRequests;
  }


  public double getNumTotalRequests() {
    return this.numTotalRequests;
  }


  public VotesMap getVotesMap() {
    return votesMap;
  }

  @Override
  public String toString() {
    return "LocationBasedDemandProfile{" + "interArrivalTime="
            + interArrivalTime + ", lastRequestTime=" + lastRequestTime
            + ", numRequests=" + numRequests + ", numTotalRequests=" + numTotalRequests
            + ", lastReconfiguredProfile=" + lastReconfiguredProfile + ", votesMap="
            + votesMap + ", lookupCount=" + lookupCount + ", updateCount=" + updateCount + '}';
  }


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


  public static void main(String[] args) throws UnknownHostException {
    LocationBasedDemandProfile dp = new LocationBasedDemandProfile();
    LOG.info(dp.toString());
    LOG.info(dp.clone().toString());
    testThings(dp);
  }

  private static class LocationBasedDemandProfileTest extends DefaultTest {

    @Test
    public void testShouldReconfigure() {
      LocationBasedDemandProfile dp = new LocationBasedDemandProfile();

    }
  }

  private static void testThings(LocationBasedDemandProfile dp) throws UnknownHostException {
    LOG.log(Level.INFO, "0,0,3 = {0}", dp.computeNumberOfReplicas(0, 0, 3));
    LOG.log(Level.INFO, "20,0,3 = {0}", dp.computeNumberOfReplicas(20, 0, 3));
    LOG.log(Level.INFO, "0,20,3 = {0}", dp.computeNumberOfReplicas(0, 20, 3));
    LOG.log(Level.INFO, "20,20,3 = {0}", dp.computeNumberOfReplicas(20, 20, 3));
    LOG.log(Level.INFO, "2000,100,3 = {0}", dp.computeNumberOfReplicas(2000, 100, 3));
    LOG.log(Level.INFO, "100,200,3 = {0}", dp.computeNumberOfReplicas(100, 2000, 3));
    LOG.log(Level.INFO, "0,0,200 = {0}", dp.computeNumberOfReplicas(0, 0, 200));
    LOG.log(Level.INFO, "20,0,200 = {0}", dp.computeNumberOfReplicas(20, 0, 200));
    LOG.log(Level.INFO, "0,20,200 = {0}", dp.computeNumberOfReplicas(0, 20, 200));
    LOG.log(Level.INFO, "20,20,200 = {0}", dp.computeNumberOfReplicas(20, 20, 200));
    LOG.log(Level.INFO, "2000,100,200 = {0}", dp.computeNumberOfReplicas(2000, 100, 200));
    LOG.log(Level.INFO, "100,200,200 = {0}", dp.computeNumberOfReplicas(100, 2000, 200));
    LOG.log(Level.INFO, "10000,200,200 = {0}", dp.computeNumberOfReplicas(10000, 200, 200));

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

    LOG.log(Level.INFO, "closest to 128.119.1.1 {0}", 
            dp.findActiveReplicaClosestToSender(InetAddress.getByName("128.119.1.1"), allActives));
    LOG.log(Level.INFO, "closest to 128.119.10.1 {0}", 
            dp.findActiveReplicaClosestToSender(InetAddress.getByName("128.119.10.1"), allActives));

    // Try it with various numbers of active replicas needed
    LOG.log(Level.INFO, "need 3: {0}", dp.pickNewActiveReplicas(3, curActives, topN, allActives));

    LOG.log(Level.INFO, "need 4: {0}", dp.pickNewActiveReplicas(4, curActives, topN, allActives));

    LOG.log(Level.INFO, "need 5: {0}", dp.pickNewActiveReplicas(5, curActives, topN, allActives));

    LOG.log(Level.INFO, "need 6: {0}", dp.pickNewActiveReplicas(6, curActives, topN, allActives));

    LOG.log(Level.INFO, "need 7: {0}", dp.pickNewActiveReplicas(7, curActives, topN, allActives));

  }
}
