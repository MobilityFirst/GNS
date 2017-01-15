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
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor;

import static edu.umass.cs.gnscommon.utils.NetworkUtils.getLocalHostLANAddress;
import edu.umass.cs.gnsserver.gnsapp.GNSApp;
import edu.umass.cs.gnsserver.gnsapp.GNSClientInternal;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.Admintercessor;
import edu.umass.cs.gnsserver.nodeconfig.GNSNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * Implements basic functionality needed by servers to handle client type requests.
 * Abstracts out the storing of request info, caching and communication needs of
 * a node.
 *
 * @author Westy
 */
public class ClientRequestHandler implements ClientRequestHandlerInterface {

  private final GNSClientInternal internalClient;
  private final Admintercessor admintercessor;

  /**
   * GNS node config object used by LNS to toString node information, such as IP, Port, ping latency.
   */
  private final GNSNodeConfig<String> gnsNodeConfig;

  private final ConsistentReconfigurableNodeConfig<String> nodeConfig;

  /**
   * Host address of the local name server.
   */
  private final InetSocketAddress nodeAddress;
  //
  private final String activeReplicaID;
  private final GNSApp app;
  private int httpServerPort;
  private int httpsServerPort;

  /**
   * Creates an instance of the ClientRequestHandler.
   *
   * @param admintercessor
   * @param nodeAddress
   * @param activeReplicaID
   * @param app
   * @param gnsNodeConfig
   * @throws java.io.IOException
   */
  public ClientRequestHandler(Admintercessor admintercessor,
          InetSocketAddress nodeAddress,
          String activeReplicaID,
          GNSApp app,
          GNSNodeConfig<String> gnsNodeConfig) throws IOException {

    assert (activeReplicaID != null);
    this.admintercessor = admintercessor;
    this.nodeAddress = nodeAddress;
    // a little hair to convert fred to fred-activeReplica if we just get fred
    this.activeReplicaID = gnsNodeConfig.isActiveReplica(activeReplicaID) ? activeReplicaID
            : gnsNodeConfig.getReplicaNodeIdForTopLevelNode(activeReplicaID);
    this.internalClient = (GNSClientInternal) new GNSClientInternal(app.toString()).setForceCoordinatedReads(true).setNumRetriesUponTimeout(1);
    this.app = app;
    // FOR NOW WE KEEP BOTH
    this.nodeConfig = new ConsistentReconfigurableNodeConfig<>(gnsNodeConfig);
    this.gnsNodeConfig = gnsNodeConfig;
  }
  
  @Override
  public GNSNodeConfig<String> getGnsNodeConfig() {
    return gnsNodeConfig;
  }

  @Override
  public ConsistentReconfigurableNodeConfig<String> getNodeConfig() {
    return nodeConfig;
  }

  @Override
  public InetSocketAddress getNodeAddress() {
    return nodeAddress;
  }

  @Override
  public String getActiveReplicaID() {
    return activeReplicaID;
  }

  @Override
  public Admintercessor getAdmintercessor() {
    return admintercessor;
  }

  @Override
  public GNSApp getApp() {
    return app;
  }

  @Override
  public int getHttpServerPort() {
    return httpServerPort;
  }

  @Override
  public void setHttpServerPort(int httpServerPort) {
    this.httpServerPort = httpServerPort;
  }

  @Override
  public String getHttpServerHostPortString() throws UnknownHostException {
    // this isn't going to work behind NATs but for public servers it should get the
    // job done
    return getLocalHostLANAddress().getHostAddress() + ":" + httpServerPort;
  }

  @Override
  public int getHttpsServerPort() {
    return httpsServerPort;
  }

  @Override
  public void setHttpsServerPort(int httpServerPort) {
    this.httpsServerPort = httpServerPort;
  }

  @Override
  public String getHttpsServerHostPortString() throws UnknownHostException {
    // this isn't going to work behind NATs but for public servers it should get the
    // job done
    return getLocalHostLANAddress().getHostAddress() + ":" + httpsServerPort;
  }

  @Override
  public GNSClientInternal getInternalClient() {
    return this.internalClient;
  }

}
