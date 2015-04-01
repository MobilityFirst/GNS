package edu.umass.cs.gns.reconfiguration.reconfigurationutils;

import java.net.InetAddress;
import java.util.ArrayList;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.reconfiguration.InterfaceRequest;
import edu.umass.cs.gns.util.Util;
import java.util.logging.Logger;

/**
 * This class maintains the demand profile for a single name.
 * It will select active name servers based on location of the demand.
 * It is based on the old LocationBasedReplication class, but doesn't
 * include checks for unreachable active servers or highly loaded servers.
 *
 * @author Westy
 */
public class LocationBasedDemandProfile extends AbstractDemandProfile {
  
  private static final Logger LOG = Logger.getLogger(LocationBasedDemandProfile.class.getName());
  
  public enum Keys {

    SERVICE_NAME, STATS, RATE, NUM_REQUESTS, NUM_TOTAL_REQUESTS, VOTES_MAP
  };

  private static final int DEFAULT_NUM_REQUESTS = 1;
  private static final long MIN_RECONFIGURATION_INTERVAL = 000;
  private static final long MIN_REQUESTS_BEFORE_RECONFIGURATION = 1;

  private double interArrivalTime = 0.0;
  private long lastRequestTime = 0;
  private int numRequests = 0;
  private int numTotalRequests = 0;
  private LocationBasedDemandProfile lastReconfiguredProfile = null;
  private VotesMap votesMap = new VotesMap();

  public LocationBasedDemandProfile(String name) {
    super(name);
  }

  // deep copy constructor
  public LocationBasedDemandProfile(LocationBasedDemandProfile dp) {
    super(dp.name);
    this.interArrivalTime = dp.interArrivalTime;
    this.lastRequestTime = dp.lastRequestTime;
    this.numRequests = dp.numRequests;
    this.numTotalRequests = dp.numTotalRequests;
    this.votesMap = new VotesMap(dp.votesMap);
  }

  public LocationBasedDemandProfile(JSONObject json) throws JSONException {
    super(json.getString(Keys.SERVICE_NAME.toString()));
    this.interArrivalTime = 1.0 / json.getDouble(Keys.RATE.toString());
    this.numRequests = json.getInt(Keys.NUM_REQUESTS.toString());
    this.numTotalRequests = json.getInt(Keys.NUM_TOTAL_REQUESTS.toString());
    this.votesMap = new VotesMap(json.getJSONObject(Keys.VOTES_MAP.toString()));
    //LOG.info("%%%%%%%%%%%%%%%%%%%%%%%%%>>> " + this.name + " VOTES MAP AFTER READ: " + this.votesMap);
  }

  public static LocationBasedDemandProfile createDemandProfile(String name) {
    return new LocationBasedDemandProfile(name);
  }

  @Override
  public void register(InterfaceRequest request, InetAddress sender) {
    if (!request.getServiceName().equals(this.name)) {
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
    this.votesMap.increment(sender);
    //LOG.info("%%%%%%%%%%%%%%%%%%%%%%%%%>>> " + this.name + " VOTES MAP: " + this.votesMap);
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

  @Override
  public boolean shouldReport() {
    if (getNumRequests() >= DEFAULT_NUM_REQUESTS) {
      return true;
    }
    return false;
  }

  public VotesMap getVotesMap() {
    return votesMap;
  }

  @Override
  public JSONObject getStats() {
    //LOG.info("%%%%%%%%%%%%%%%%%%%%%%%%%>>> " + this.name + " VOTES MAP BEFORE GET STATS: " + this.votesMap);
    JSONObject json = new JSONObject();
    try {
      json.put(Keys.SERVICE_NAME.toString(), this.name);
      json.put(Keys.RATE.toString(), getRequestRate());
      json.put(Keys.NUM_REQUESTS.toString(), getNumRequests());
      json.put(Keys.NUM_TOTAL_REQUESTS.toString(), getNumTotalRequests());
      json.put(Keys.VOTES_MAP.toString(), getVotesMap().toJSONObject());
    } catch (JSONException je) {
      je.printStackTrace();
    }
    //LOG.info("%%%%%%%%%%%%%%%%%%%%%%%%%>>> " + this.name + " GET STATS: " + json);
    return json;
  }

  @Override
  public void reset() {
    this.interArrivalTime = 0.0;
    this.lastRequestTime = 0;
    this.numRequests = 0;
    this.votesMap = new VotesMap();
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
    this.votesMap.combine(update.getVotesMap());
    //LOG.info("%%%%%%%%%%%%%%%%%%%%%%%%%>>> " + this.name + " VOTES MAP AFTER COMBINE: " + this.votesMap);
  }

  @Override
  public ArrayList<InetAddress> shouldReconfigure(ArrayList<InetAddress> curActives) {
    if (this.lastReconfiguredProfile != null) {
      if (System.currentTimeMillis()
              - this.lastReconfiguredProfile.lastRequestTime < MIN_RECONFIGURATION_INTERVAL) {
        return null;
      }
      if (this.numTotalRequests
              - this.lastReconfiguredProfile.numTotalRequests < MIN_REQUESTS_BEFORE_RECONFIGURATION) {
        return null;
      }
    }
    LOG.info("%%%%%%%%%%%%%%%%%%%%%%%%%>>> " + this.name + " VOTES MAP: " + this.votesMap + " TOP: " + this.votesMap.getTopN(10));
    
    return curActives;
  }

  @Override
  public void justReconfigured() {
    this.lastReconfiguredProfile = this.clone();
  }
}
