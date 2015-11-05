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

import edu.umass.cs.gigapaxos.interfaces.Replicable;

/**
 * @author V. Arun
 */

public interface ReplicableDeprecated extends Application, Replicable {

	// Application.handleDecision will soon not take the third argument
	@Override
	public boolean handleDecision(String name, String value,
			boolean doNotReplyToClient);

	public String getState(String name);

	@Override
	public boolean updateState(String name, String state);

}
