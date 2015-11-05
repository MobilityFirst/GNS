/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.gigapaxos.deprecated;

import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.Replicable;

import java.util.Set;

/**
 *  This abstract class is deprecated and was used mainly to transition from the 
 * old paxos implementation to the new one. It currently exists only for
 * backwards compatibility reasons.
 * @param <NodeIdType> 
 */
@Deprecated
public abstract class AbstractPaxosManager<NodeIdType> {

  /**
   *
   * @param paxosIDNoVersion paxos ID of paxos instance excluding the version number
   * @param version number of times member set has been changed for this paxos ID
   * @param nodeIDs node IDs of all members
   * @param paxosInterface initial state of data which this paxos instance is managing.
   * @return true if paxos instance is created. false if another instance with same ID and version already exists, or
   * size of nodeIDs is less than 3.
   */
  public abstract boolean createPaxosInstance(String paxosIDNoVersion, short version, Set<NodeIdType> nodeIDs, Replicable paxosInterface);

  /**
 * @param paxosID
 * @return The set of nodes corresponding to {@code paxosID}.
 */
public abstract Set<NodeIdType> getPaxosNodeIDs(String paxosID);
  /**
   * Propose requestPacket in the paxos instance with the given paxosID.
   *
   * ReqeustPacket.clientID is used to distinguish which method proposed this value.
   * @param paxosIDNoVersion paxosID of the paxos group excluding version number
   * @param value request to be proposed
   * @return NULL if no paxos instance with given paxos ID was found, returns paxosIDNoVersion otherwise.
   */
  public abstract String propose(String paxosIDNoVersion, String value);

  /**
 * @param paxosID
 * @param value
 * @param version
 * @return The paxosID and version to which this stop was issued.
 */
public abstract String proposeStop(String paxosID, String value, short version);


  /**
   * Handle incoming message for any Paxos instance as well as failure detection messages.
   * @param json json object received
   */
  public abstract void handleIncomingPacket(JSONObject json);


  /**
   * Deletes all paxos instances and paxos logs.
   */
  public abstract void resetAll();


}
