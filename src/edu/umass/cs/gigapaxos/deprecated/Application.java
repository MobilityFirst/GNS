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

/**
 * @author V. Arun
 */

public interface Application {
	/**
	 * An atomic operation that either completes and returns true or throws an
	 * exception. If the method does not return true, paxos will call this
	 * method again until it returns true.
	 * 
	 * @param name
	 * @param value
	 * @param doNotReplyToClient
	 * @return Should always return true or throw an exception. The paxos
	 *         replicated state machine can not make progress otherwise.
	 */
	public boolean handleDecision(String name, String value,
			boolean doNotReplyToClient);
}
