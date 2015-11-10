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
package edu.umass.cs.gnsserver.gnsApp.noopTest;


import edu.umass.cs.gnsserver.nodeconfig.HostFileLoader;
import edu.umass.cs.gnsserver.nodeconfig.HostSpec;
import java.util.HashSet;
import java.util.Set;

import edu.umass.cs.reconfiguration.interfaces.ReconfigurableNodeConfig;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONException;

/**
 * @author Westy adapted from code originally by V. Arun
 */
public class TestReconfigurableNodeConfig extends TestNodeConfig<String>
        implements ReconfigurableNodeConfig<String> {

  private static final String RC_SUFFIX = "recon";
  private static final int RC_PORT_OFFSET = 1;
  
  private Logger log = Logger.getLogger(getClass().getName());

  /**
   * Creates a TestReconfigurableNodeConfig instance.
   * 
   * @param hostsFile
   * @throws java.io.IOException
   */
  public TestReconfigurableNodeConfig(String hostsFile) throws IOException {
    super();
    readHostsFile(hostsFile);
  }

  /**
   * @param hostsFile
   * @param defaultPort
   * @throws java.io.IOException
   */
  public TestReconfigurableNodeConfig(String hostsFile, int defaultPort) throws IOException {
    super(defaultPort);
    readHostsFile(hostsFile);
  }

  @Override
  public Set<String> getActiveReplicas() {
    return this.getNodeIDs();
  }

  /* Add node IDs for RCs that are derived from active node IDs.
   */
  @Override
  public Set<String> getReconfigurators() {
    Set<String> actives = this.getNodeIDs();
    Set<String> generatedRCs = new HashSet<String>();
    for (String id : actives) {
      generatedRCs.add(activeToRC(id));
    }
    return generatedRCs;
  }

  /* Either id exists as active or exists as RC, i.e., the 
   * corresponding active exists in the latter case.
   */
  @Override
  public boolean nodeExists(String id) {
    boolean activeExists = super.nodeExists(id);
    boolean RCExists = super.nodeExists(RCToActive(id));
    return activeExists || RCExists;
  }

  @Override
  public InetAddress getNodeAddress(String id) {
    if (super.nodeExists(id)) {
      return super.getNodeAddress(id);
    } else {
      return super.getNodeAddress(RCToActive(id));
    }
  }
  
  @Override
  public InetAddress getBindAddress(String id) {
    if (super.nodeExists(id)) {
      return super.getBindAddress(id);
    } else {
      return super.getBindAddress(RCToActive(id));
    }
  }

  @Override
  public int getNodePort(String id) {
    // If it's in the node list it's an AR, otherwise
    // it's and RC so lookup AR from RC and add offest to AR port
    if (super.nodeExists(id)) {
      // it's an AR
      return super.getNodePort(id);
    } else {
      // it's an RC
      return super.getNodePort(RCToActive(id)) + RC_PORT_OFFSET;
    }
  }

  @Override
  public Set<String> getValuesFromStringSet(Set<String> strNodes) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Set<String> getValuesFromJSONArray(JSONArray array)
          throws JSONException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  /**
   *
   * @param activeID
   * @return a string
   */
  public String activeToRC(String activeID) {
    return activeID + "_" + RC_SUFFIX;
  }

  /**
   *
   * @param rcID
   * @return a string
   */
  public String RCToActive(String rcID) {
    int index;
    if ((index = rcID.lastIndexOf("_")) != -1) {
      return rcID.substring(0, index);
    } else {
      log.severe("Bad RC id:" + rcID);
      // this is probably going to cause issues...
      return null;
    }
  }

  private void readHostsFile(String hostsFile) throws IOException {
    List<HostSpec> hosts = null;
    try {
      hosts = HostFileLoader.loadHostFile(hostsFile);
    } catch (Exception e) {
      e.printStackTrace();
      throw new IOException("Problem loading hosts file: " + e);
    }
    for (HostSpec spec : hosts) {
      System.out.println("For node " + spec.getId()
              + ":  public name => " + spec.getName()
              + " public ip => " + spec.getExternalIP());
      add(spec.getId(), 
              InetAddress.getByName(spec.getExternalIP())
              //InetAddress.getByName(spec.getName())
      );
    }

  }

}
