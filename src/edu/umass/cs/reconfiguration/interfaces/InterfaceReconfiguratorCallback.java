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
package edu.umass.cs.reconfiguration.interfaces;

import edu.umass.cs.gigapaxos.InterfaceRequest;

/**
 * @author V. Arun
 *         <p>
 *         The method executed(.) in this interface is called only by
 *         AbstractReplicaCoordinator that is an internal class. This interface
 *         is not (yet) expected to be implemented by a third-party class like
 *         an instance of Application.
 */
public interface InterfaceReconfiguratorCallback {
	/**
	 * @param request
	 * @param handled
	 */
	public void executed(InterfaceRequest request, boolean handled);
	/**
	 * @param request
	 */
	public void preExecuted(InterfaceRequest request);
}
