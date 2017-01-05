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

import edu.umass.cs.gnsserver.gnsapp.GNSApp;
import edu.umass.cs.gnsserver.gnsapp.GNSClientInternal;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.Admintercessor;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.RemoteQuery;
import edu.umass.cs.gnsserver.nodeconfig.GNSNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 **
 * An interface for handling of client requests, comms and cacheing.
 * Abstracts out the methods for storing of request info, caching and
 * communication needs of a node. A lot of this code used to be static methods in the
 * ClientCommandProcessor (CCP) (formerly the LocalNameServer).
 * This class makes the code that uses it not depend statically on the CCP.
 *
 */
public interface ClientRequestHandlerInterface {

  /**
   * Return the remote query handler.
   *
   * @return the remote query handler
   */
  public RemoteQuery getRemoteQuery();

  public GNSClientInternal getInternalClient();

  /**
   * Maintains information about other nodes.
   *
   * @return a GNSNodeConfig instance
   */
  public GNSNodeConfig<String> getGnsNodeConfig();

  // FIXME: During transition we have both this and the above.
  /**
   * Only used by CCPListenerAdmin. Maybe should go away.
   *
   * @return a ConsistentReconfigurableNodeConfig instance
   */
  public ConsistentReconfigurableNodeConfig<String> getNodeConfig();

  /**
   * Returns the address of this node.
   *
   * @return the address
   */
  public InetSocketAddress getNodeAddress();

  /**
   * Returns the id of the co-located active replica.
   *
   * @return Active replica name.
   */
  public String getActiveReplicaID();

  /**
   * Returns the Admintercessor.
   *
   * @return an Admintercessor instance
   */
  public Admintercessor getAdmintercessor();

  /**
   * Returns the app associated with this handler.
   *
   * @return a GnsApp instance
   */
  public GNSApp getApp();

  /**
   * Returns the port associated with the HTTP server running on this node.
   *
   * @return HTTP server port.
   */
  public int getHttpServerPort();

  /**
   * Sets the port associated with the HTTP server running on this node.
   *
   * @param port
   */
  public void setHttpServerPort(int port);

  /**
   * *
   * Returns a string of the form inetaddress:port for the http server.
   *
   * @return HTTP server:port
   * @throws java.net.UnknownHostException
   */
  public String getHttpServerHostPortString() throws UnknownHostException;

  /**
   * Returns the port associated with the secure HTTP server running on this node.
   *
   * @return HTTP server port.
   */
  public int getHttpsServerPort();

  /**
   * Sets the port associated with the secure HTTP server running on this node.
   *
   * @param port
   */
  public void setHttpsServerPort(int port);

  /**
   * *
   * Returns a string of the form inetaddress:port for the secure http server.
   *
   * @return HTTP server:port
   * @throws java.net.UnknownHostException
   */
  public String getHttpsServerHostPortString() throws UnknownHostException;

}
