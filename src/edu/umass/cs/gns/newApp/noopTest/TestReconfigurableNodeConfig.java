/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.newApp.noopTest;

import edu.umass.cs.gns.nodeconfig.HostFileLoader;
import edu.umass.cs.gns.nodeconfig.HostSpec;
import java.util.HashSet;
import java.util.Set;
import edu.umass.cs.reconfiguration.InterfaceReconfigurableNodeConfig;
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
        implements InterfaceReconfigurableNodeConfig<String> {

  private static final String RC_SUFFIX = "recon";
  private static final int RC_PORT_OFFSET = 1;
  
  private Logger log = Logger.getLogger(getClass().getName());

  /**
   *
   */
  public TestReconfigurableNodeConfig(String hostsFile) throws IOException {
    super();
    readHostsFile(hostsFile);
  }

  /**
   * @param defaultPort
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

  public String activeToRC(String activeID) {
    return activeID + "_" + RC_SUFFIX;
  }

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
    for (HostSpec<String> spec : hosts) {
      System.out.println("For node " + spec.getId()
              + " public name: " + spec.getName()
              + " public ip: " + spec.getExternalIP());
      add(spec.getId(), InetAddress.getByName(spec.getName()));
    }

  }

}
