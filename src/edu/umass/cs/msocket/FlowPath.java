/**
 * Mobility First - Global Name Resolution Service (GNS)
 * Copyright (C) 2013 University of Massachusetts - Emmanuel Cecchet.
 * Contact: cecchet@cs.umass.edu
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 *
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): ______________________.
 */

package edu.umass.cs.msocket;

import java.net.SocketAddress;

/**
 * This class defines a FlowPath
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class FlowPath
{
  // changed it to flowPathId, historically
  // flowID means the overall unique flowID between the server
  // and the client in cinfo or mSocket.
  private int           flowPathId;
  private SocketAddress localEndpoint;
  private SocketAddress remoteEndpoint;

  /**
   * Creates a new <code>FlowPath</code> object
   * 
   * @param flowId the unique flow path id
   * @param localEndpoint local socket address for this flow path
   * @param remoteEndpoint remote socket address for this flow path
   */
  public FlowPath(int flowPathId, SocketAddress localEndpoint, SocketAddress remoteEndpoint)
  {
    this.flowPathId = flowPathId;
    this.localEndpoint = localEndpoint;
    this.remoteEndpoint = remoteEndpoint;
  }

  /**
   * Returns the flowPathId value.
   * @return Returns the flowPathId.
   */
  public int getFlowPathId()
  {
    return flowPathId;
  }
  
  /**
   * Returns the localEndpoint value.
   * 
   * @return Returns the localEndpoint.
   */
  public SocketAddress getLocalEndpoint()
  {
    return localEndpoint;
  }
  
  /**
   * Returns the remoteEndpoint value.
   * 
   * @return Returns the remoteEndpoint.
   */
  public SocketAddress getRemoteEndpoint()
  {
    return remoteEndpoint;
  }

  /**
   * Sets the flowPathId value.
   * @param flowPathId The flowPathId to set.
   */
//  private void setFlowPathId(int flowPathId)
//  {
//    this.flowPathId = flowPathId;
//  }

  /**
   * Sets the localEndpoint value.
   * 
   * @param localEndpoint The localEndpoint to set.
   */
//  private void setLocalEndpoint(SocketAddress localEndpoint)
//  {
//    this.localEndpoint = localEndpoint;
//  }

  /**
   * Sets the remoteEndpoint value.
   * 
   * @param remoteEndpoint The remoteEndpoint to set.
   */
//  private void setRemoteEndpoint(SocketAddress remoteEndpoint)
//  {
//    this.remoteEndpoint = remoteEndpoint;
//  }
}