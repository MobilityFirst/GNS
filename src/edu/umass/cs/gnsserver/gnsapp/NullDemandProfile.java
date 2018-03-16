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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.gnsapp;

import java.net.InetAddress;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableAppInfo;
import edu.umass.cs.reconfiguration.reconfigurationutils.AbstractDemandProfile;

/**
 * @author Westy
 */
/*
 * This class maintains a demand profile that neither wants reports or reconfigurations.
 *
 * FIXME: Created this in part because the default profile was resulting in a reconfiguration that
 * was either not working or just exacerbating issues that we were trying to fix while debugging.
 * So we need to verify that the default DemandProfile works ok with our stuff instead of this one.
 */
public class NullDemandProfile extends AbstractDemandProfile {

  /**
   * The keys used in NullDemandProfile.
   */
  public enum Keys {

    /**
     * SERVICE_NAME
     */
    SERVICE_NAME
  };

  /**
   * Creates a NullDemandProfile instance.
   * 
   * @param name
   */
  public NullDemandProfile(String name) {
    super(name);
  }

  /**
   * Creates a NullDemandProfile by doing a deep copy of another instance.
   * 
   * @param dp
   */
    public NullDemandProfile(NullDemandProfile dp) {
    super(dp.name);
  }

  /**
   * Creates a NullDemandProfile from a JSON packet.
   * 
   * @param json
   * @throws JSONException
   */
  public NullDemandProfile(JSONObject json) throws JSONException {
    super(json.getString(Keys.SERVICE_NAME.toString()));
  }

  /**
   * Creates an empty NullDemandProfile.
   * 
   * @param name
   * @return a NullDemandProfile
   */
  public static NullDemandProfile createDemandProfile(String name) {
    return new NullDemandProfile(name);
  }

  /**
   *
   * @param request
   * @param sender
   * @param nodeConfig
   */
  @Override
  public boolean shouldReportDemandStats(Request request, InetAddress sender, ReconfigurableAppInfo nodeConfig) {
	    return false;
  }


  /**
   *
   * @return the stats
   */
  @Override
  public JSONObject getDemandStats() {
    JSONObject json = new JSONObject();
    try {
      json.put(Keys.SERVICE_NAME.toString(), this.name);
    } catch (JSONException je) {
      je.printStackTrace();
    }
    return json;
  }

  /**
   * Reset this.
   */
  @Override
  public void reset() {
  }

  @Override
  public NullDemandProfile clone() {
    return new NullDemandProfile(this);
  }

  /**
   *
   * @param dp
   */
  @Override
  public void combine(AbstractDemandProfile dp) {
  }

  /**
   *
   * @param curActives
   * @param nodeConfig
   * @return  a list of addresses we should reconfigure
   */
  @Override
  public Set<String> reconfigure(Set<String> curActives, 
          ReconfigurableAppInfo nodeConfig) {
    return null;
  }

  /**
   * * Was this just rconfigured.
   */
  @Override
  public void justReconfigured() {
  }
}
