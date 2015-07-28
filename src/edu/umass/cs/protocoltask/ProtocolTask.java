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

import java.util.Set;

import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.utils.Keyable;

/**
 * @author V. Arun
 * @param <NodeIDType>
 * @param <EventType>
 * @param <KeyType>
 * 
 *            A protocol task is broadly a task consisting of a sequence of
 *            asynchronous steps, where an asynchronous step is one that needs
 *            to wait for some message(s) from remote node(s) in order to
 *            finish.
 * 
 *            A protocol task is represented as an extended Finite State Machine
 *            as this allows us to implement a sequence of event/action steps.
 *            We call it an extended FSM because an FSM state, strictly
 *            speaking, does not have "memory"; or if it needs to have memory,
 *            we need to create a separate state for each value the
 *            corresponding register could take. But it is convenient to think
 *            of most event/action protocols as FSMs whose states have some
 *            memory, e.g., a counter in a "wait_for_replies" state that is
 *            incremented with each new reply receipt without the state itself
 *            changing until a threshold number of replies are received.
 * 
 *            What is this abstraction good for? The following use cases:
 * 
 *            (1) Retransmission until delivered. Note: With reliable NIO, you
 *            should be extremely careful with retransmissions. We should really
 *            just let NIO handle most retransmissions, otherwise there is a
 *            risk of recklessly overloading the system. This interface will
 *            take care of retransmissions internally. If this interface can
 *            not, then no one else can, and re-trying yourself is futile.
 * 
 *            (2) Getting a value from a remote node.
 * 
 *            (3) Getting replies from a threshold subset of nodes.
 * 
 *            (4) A sequence of any of the above steps.
 */

public interface ProtocolTask<NodeIDType, EventType, KeyType> extends
		Keyable<KeyType> {

	/**
	 * The action handling the event may return messaging tasks that will be
	 * automatically handled by this interface. This interface lets the protocol
	 * developer focus on event/action pairs without worrying about networking
	 * or scheduling optimizations. The parameter ptasks[0] returns a single
	 * protocol task if any spawned by this action.
	 * 
	 * We need a unique key ***that is unique across all nodes*** in order to
	 * match incoming messages against the corresponding task. ProtocolExecutor
	 * automatically inserts this key in every message exchanged as part of the
	 * protocol task. The getKey method is necessitated by the Keyable<KeyType>
	 * interface in this class. A simple way to choose a String key that is
	 * unique across all nodes at a node is to use the current node's ID
	 * concatenated with a random number and/or the current time. A protocol
	 * task can also explicitly choose meaningful (i.e., not random or based on
	 * timestamps) names for tasks, but they still need to be unique across all
	 * nodes. Refer to the examples provided with the protocoltask package.
	 * 
	 * A protocol task can be canceled from within the task, if necessary, by
	 * invoking ProtocolExecutor.cancel(this). But you probably have a bad
	 * design if you need to rely on this.
	 * 
	 * @param event
	 * @param ptasks
	 * @return Returns messaging tasks to be performed.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleEvent(
			ProtocolEvent<EventType, KeyType> event,
			ProtocolTask<NodeIDType, EventType, KeyType>[] ptasks);

	/**
	 * Actions executed in the beginning.
	 * 
	 * @return Initial messaging task.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] start();

	/**
	 * The refreshKey() method is outdated has been removed now.
	 * ProtocolExecutor now just throws an exception if the caller tries to
	 * insert a task with a duplicate key. It is the caller's responsibility to
	 * either choose a unique key (across all nodes) or use the
	 * ProtocolExecutor.spawnIfNotRunning method. The latter option is not
	 * available for ptasks[0] returned by the ProtocolTask.handleEvent method,
	 * but the caller can always use ProtocolExecutor.isRunning to check if a
	 * task with the same key is already running.
	 * 
	 * @return The key.
	 */

	// public KeyType refreshKey();

	/**
	 * @return The event types processed by this protocol task.
	 */
	public Set<EventType> getEventTypes();

}
