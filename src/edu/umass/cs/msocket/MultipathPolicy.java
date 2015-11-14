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

/**
 * This class defines a MultipathPolicy
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public enum MultipathPolicy
{
  /**
   * Multipath policy to send data randomly among multiple paths
   */
  MULTIPATH_POLICY_RANDOM,
  /**
   * Default multipath policy (optimized multipath policy)
   */
  MULTIPATH_POLICY_ROUNDROBIN,
  /**
   * Multipath policy doing a round robin over all paths
   */
  MULTIPATH_POLICY_UNIFORM,
  /**
   * Multipath policy. TODO: ???
   */
  MULTIPATH_POLICY_OUTSTAND_RATIO,
  /**
   * policy to optimize retransmissions
   */
  MULTIPATH_POLICY_RTX_OPT,
  /**
   * Full duplication on both paths
   */
  MULTIPATH_POLICY_FULL_DUP,
  /**
   * Black box writing policy
   */
  MULTIPATH_POLICY_BLACKBOX
}