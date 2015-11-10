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
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport;

import edu.umass.cs.gnsserver.gnsApp.GnsApp;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.Admintercessor;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.Intercessor;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.RequestHandlerParameters;
import edu.umass.cs.gnsserver.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gnsserver.gnsApp.packet.SelectRequestPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import org.json.JSONObject;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.json.JSONException;

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
   * The executor that runs tasks.
   *
   * @return the executorService
   */
  public ScheduledThreadPoolExecutor getExecutorService();

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
   * Returns that set of parameters used to control the handlers behavior.
   *
   * @return the parameters
   */
  public RequestHandlerParameters getParameters();

  /**
   * Returns the address of this node.
   *
   * @return the address
   */
  public InetSocketAddress getNodeAddress();

  /**
   * Returns the id of the co-located active replica.
   * 
   * @return 
   */
  public String getActiveReplicaID();

  /**
   * Returns the intercessor.
   * 
   * @return an Intercessor instance
   */
  public Intercessor getIntercessor();

  /**
   * Returns the Admintercessor.
   * 
   * @return an Admintercessor instance
   */
  public Admintercessor getAdmintercessor();

  // REQUEST INFO METHODS
  
  /**
   * Returns a new unique request id.
   * 
   * @return 
   */
  public int getUniqueRequestID();

  /**
   * Adds a new RequestInfo entry to the map for the query id.
   * @param id
   * @param requestInfo 
   */
  public void addRequestInfo(int id, RequestInfo requestInfo);

  /**
   **
   * Removes and returns RequestInfo entry from the map for a query Id..
   *
   * @param id Query Id
   * @return an query id
   */
  public RequestInfo removeRequestInfo(int id);

  /**
   * Returns the update info for id.
   *
   * @param id
   * @return the requestinfo
   */
  public RequestInfo getRequestInfo(int id);

  /**
   * Adds information of a transmitted select to a query transmitted map.
   *
   * @param recordKey
   * @param incomingPacket
   * @return the id
   */
  public int addSelectInfo(String recordKey, SelectRequestPacket<String> incomingPacket);

  /**
   * @param id
   * @return the id
   */
  public SelectInfo removeSelectInfo(int id);

  /**
   * Returns the select info for id.
   *
   * @param id
   * @return the id
   */
  public SelectInfo getSelectInfo(int id);

  // NETWORK METHODS
  /**
   **
   * Return a Set containing ids of primary replica for <i>name</i>
   *
   * @param name
   * @return a set of ids
   */
  public Set<String> getReplicaControllers(String name);

  /**
   **
   * Returns the closest primary name server for <i>name</i>.
   *
   * @param name
   * @param nameServersQueried
   * @return Closest primary name server for <i>name</i>, or -1 if no such name server is present.
   *
   */
  public String getClosestReplicaController(String name, Set<String> nameServersQueried);

  /**
   * Send packet to NS
   *
   * @param json
   * @param ns
   */
  public void sendToNS(JSONObject json, String ns);

  /**
   * Send a JSON packet to an IP address / port.
   *
   * @param json
   * @param address
   * @param port
   */
  public void sendToAddress(JSONObject json, String address, int port);

  /**
   * Instrumentation - Updates various instrumentation including the request counter and requests per second
   */
  public void updateRequestStatistics();

  /**
   * Instrumentation - Return the request counter.
   *
   * @return a long
   */
  public long getReceivedRequests();

  /**
   * Instrumentation - Return the requests per second measure.
   *
   * @return an int
   */
  public int getRequestsPerSecond();

  /**
   * Returns true if update packets will be sent to the co-located replica instead of being handled locally.
   *
   * returns true or false
   * @return true or false
   */
  public boolean reallySendUpdateToReplica();

  /**
   * Returns set the value which determines if update packets will be sent to the co-located replica instead of being handled locally.
   * @param reallySend
   */
  public void setReallySendUpdateToReplica(boolean reallySend);

  // Below are for the new app
  /**
   * Returns a randomly selected active replica.
   *
   * @return an active replica
   */
  public String getRandomReplica();

  /**
   * Returns a randomly selected reconfigurator.
   *
   * @return a reconfigurator
   */
  public String getRandomReconfigurator();

  /**
   * Returns the first active replica.
   *
   * @return an active replica
   */
  public String getFirstReplica();

  /**
   * Returns a this first reconfigurator.
   *
   * @return a reconfigurator
   */
  public String getFirstReconfigurator();

  /**
   * Sends a ReconfigurationPacket to a reconfigurator.
   * 
   * @param req - the request
   * @param id - the id of the reconfigurator
   * @throws JSONException
   * @throws IOException 
   */
  public void sendRequestToReconfigurator(BasicReconfigurationPacket req, String id) throws JSONException, IOException;

  /**
   * Invokes the handleEvent method of the associated protocol executor.
   *
   * @param json
   * @return a boolean indicating if the request was actually handled
   * @throws JSONException
   */
  public boolean handleEvent(JSONObject json) throws JSONException;

  /**
   * Adds a mapping between a ServiceName request and a CCPREquestID.
   * Provides backward compatibility between old Add and Remove record code and new name service code.
   *
   * @param name
   * @param id
   */
  public void addCreateRequestNameToIDMapping(String name, int id);

  /**
   * Looks up the mapping between a ServiceName request and a CCPREquestID.
   * Provides backward compatibility between old Add and Remove record code and new name service code.
   *
   * @param name
   * @return the request id
   */
  public Integer getCreateRequestNameToIDMapping(String name);
  
  /**
   * Returns true if there are no more outstanding create requests for the given id.
   * 
   * @param id
   * @return true if there are no more outstanding create requests
   */
  public boolean pendingCreatesIsEmpty(int id);
  
  /**
   * Removes the mapping between a ServiceName request and a CCPREquestID.
   * Provides backward compatibility between old Add and Remove record code and new name service code.
   *
   * @param name
   * @return the request id or null if if can't be found
   */
  public Integer removeCreateRequestNameToIDMapping(String name);

  /**
   * Adds a mapping between a ServiceName request and a CCPREquestID.
   * Provides backward compatibility between old Add and Remove record code and new name service code.
   *
   * @param name
   * @param id
   */
  public void addDeleteRequestNameToIDMapping(String name, int id);

  /**
   * Looks up the mapping between a ServiceName request and a CCPREquestID.
   * Provides backward compatibility between old Add and Remove record code and new name service code.
   *
   * @param name
   * @return the request id
   */
  public Integer getDeleteRequestNameToIDMapping(String name);

  /**
   * Removes the mapping between a ServiceName request and a CCPREquestID.
   * Provides backward compatibility between old Add and Remove record code and new name service code.
   *
   * @param name
   * @return the request id or null if if can't be found
   */
  public Integer removeDeleteRequestNameToIDMapping(String name);

  /**
   * Adds a mapping between a ServiceName request and a CCPREquestID.
   * Provides backward compatibility between old Add and Remove record code and new name service code.
   *
   * @param name
   * @param id
   */
  public void addActivesRequestNameToIDMapping(String name, int id);

  /**
   * Looks up the mapping between a ServiceName request and a CCPREquestID.
   * Provides backward compatibility between old Add and Remove record code and new name service code.
   *
   * @param name
   * @return the request id
   */
  public Integer getActivesRequestNameToIDMapping(String name);

  /**
   * Removes the mapping between a ServiceName request and a CCPREquestID.
   * Provides backward compatibility between old Add and Remove record code and new name service code.
   *
   * @param name
   * @return the request if or null if if can't be found
   */
  public Integer removeActivesRequestNameToIDMapping(String name);

  /**
   * Returns the app associated with this handler.
   *
   * @return a GnsApp instance
   */
  public GnsApp getApp();
}
