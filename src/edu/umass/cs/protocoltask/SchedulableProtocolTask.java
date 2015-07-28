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
package edu.umass.cs.protocoltask;

import edu.umass.cs.nio.GenericMessagingTask;

/**
 * @author V. Arun
 * @param <NodeIDType>
 * @param <EventType>
 * @param <KeyType>
 * 
 *            Allows for a separate restart method possibly different from the
 *            first start method.
 */
public interface SchedulableProtocolTask<NodeIDType, EventType, KeyType>
		extends ProtocolTask<NodeIDType, EventType, KeyType> {
	/**
	 * @return The messaging task upon each restart.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] restart();

	/**
	 * @return The restart period.
	 */
	public long getPeriod();
}
