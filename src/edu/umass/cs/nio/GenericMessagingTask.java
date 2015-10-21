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
package edu.umass.cs.nio;

/**
 * @author V. Arun
 * @param <NodeIDType>
 * @param <MessageType>
 */
public class GenericMessagingTask<NodeIDType, MessageType> {
	/**
	 * The list of recipients. Every recipient will be sent all messages.
	 */
	public final Object[] recipients;
	/** 
	 * The list of messages. Every recipient will be sent all messages.
	 */
	public final Object[] msgs;

	/**
	 * Null recipients and messages.
	 */
	public GenericMessagingTask() {
		this.recipients = null;
		this.msgs = null;
	}

	/**
	 * Unicast
	 * 
	 * @param destID
	 * @param pkt
	 */
	public GenericMessagingTask(NodeIDType destID, MessageType pkt) {
		assert (pkt != null) : "Incorrect usage: InterfaceMessagingTask should not be instantiated with no messages";
		if (pkt == null)
			throw new RuntimeException(
					"Incorrect usage: InterfaceMessagingTask should not be instantiated with no messages");
		this.recipients = new Object[1];
		this.recipients[0] = destID;

		this.msgs = new Object[pkt == null ? 0 : 1];
		if (pkt != null)
			msgs[0] = pkt;
	}

	/**
	 * Multicast
	 * 
	 * @param destIDs
	 * @param pkt
	 */
	public GenericMessagingTask(Object[] destIDs, MessageType pkt) {
		assert (pkt != null && destIDs != null) : "Incorrect usage: InterfaceMessagingTask should not be instantiated with null messages or destinations";
		if (!(pkt != null && destIDs != null))
			throw new RuntimeException(
					"Incorrect usage: InterfaceMessagingTask should not be instantiated with null messages or destinations");
		this.recipients = destIDs;
		this.msgs = new Object[pkt == null ? 0 : 1];
		if (pkt != null)
			msgs[0] = pkt;
	}

	/**
	 * Unicast multiple packets
	 * 
	 * @param destID
	 * @param pkts
	 */
	public GenericMessagingTask(NodeIDType destID, Object[] pkts) {
		assert (pkts != null && pkts.length > 0 && pkts[0] != null) : "Incorrect usage: InterfaceMessagingTask should not be instantiated with no messages";
		if (!(pkts != null && pkts.length > 0 && pkts[0] != null))
			throw new RuntimeException(
					"Incorrect usage: InterfaceMessagingTask should not be instantiated with no messages");

		this.recipients = new Object[1];
		this.recipients[0] = destID;
		this.msgs = pkts == null ? new Object[0] : pkts;
	}

	/**
	 * Multicast multiple packets. This is a special type of constructor
	 * compared to the others above. This is the only constructor that allows
	 * InterfaceMessagingTask to be instantiated with no messages. We need this
	 * as it is convenient to create an empty InterfaceMessagingTask sometimes.
	 * 
	 * @param destIDs
	 * @param pkts
	 */
	public GenericMessagingTask(Object[] destIDs, Object[] pkts) {
		assert (pkts != null && destIDs != null) : "Incorrect usage: InterfaceMessagingTask should not be instantiated with null messages or destinations";
		if (!(pkts != null && destIDs != null))
			throw new RuntimeException(
					"Incorrect usage: InterfaceMessagingTask should not be instantiated with null messages or destinations");

		for (Object obj : pkts)
			assert (obj != null) : "Incorrect usage: InterfaceMessagingTask should not be instantiated with null messages";

		this.recipients = (destIDs == null ? new Object[0] : destIDs);
		this.msgs = pkts == null ? new Object[0] : pkts;
	}

	/**
	 * @return Returns true if either recipients or messages are null or empty.
	 */
	public boolean isEmpty() {
		return this.msgs == null || this.msgs.length == 0
				|| this.recipients == null || this.recipients.length == 0;
	}

	/**
	 * Converts the two parameters into an array of size two.
	 * 
	 * @param mtask1
	 * @param mtask2
	 * @return Returns an array of size two containing {@code mtask1} and
	 *         {@code mtask2}.
	 */
	public static GenericMessagingTask<?, ?>[] toArray(
			GenericMessagingTask<?, ?> mtask1, GenericMessagingTask<?, ?> mtask2) {
		GenericMessagingTask<?, ?>[] mtasks = new GenericMessagingTask[2];
		mtasks[0] = mtask1;
		mtasks[1] = mtask2;
		return mtasks;
	}

	/**
	 * @return Returns a single-element array containing {@code this}.
	 */
	@SuppressWarnings("unchecked")
	public GenericMessagingTask<NodeIDType, ?>[] toArray() {
		GenericMessagingTask<NodeIDType, ?>[] mtasks = new GenericMessagingTask[1];
		mtasks[0] = this;
		return mtasks;
	}

	/**
	 * @param mtasks
	 * @return Returns true if all mtasks are empty or null.
	 */
	public static boolean isEmpty(GenericMessagingTask<?, ?>[] mtasks) {
		if (mtasks == null || mtasks.length == 0)
			return true;
		boolean empty = true;
		for (GenericMessagingTask<?, ?> mtask : mtasks) {
			empty = empty && mtask.isEmpty();
		}
		return empty;
	}

	/**
	 * For pretty printing
	 */
	public String toString() {
		if (msgs.length == 0)
			return "NULL";
		String s = "";
		s += " to recipients: [";
		for (int i = 0; i < this.recipients.length; i++) {
			s += this.recipients[i] + " ";
		}
		s += "]; Message";
		s += (msgs.length > 1) ? "s:\n[" : ": ";
		for (int i = 0; i < msgs.length; i++) {
			s += (msgs.length > 1 ? "\n    " : "");
			if (i == 10 && msgs.length > 25) {
				s += ".... (skipping " + (msgs.length - 19) + ")";
				i = msgs.length - 11;
			} else
				s += msgs[i];
		}
		if (msgs.length > 1)
			s += "\n]";
		return s;
	}

	static class Main {
		public static void main(String[] args) {
			GenericMessagingTask<Integer, String> mtask = new GenericMessagingTask<Integer, String>(
					23, "Hello");
			assert (mtask.msgs.length == 1 && mtask.msgs[0].equals("Hello"));
			Integer[] nodes = { 23, 45, 66 };
			GenericMessagingTask<Integer, String> mtask1 = new GenericMessagingTask<Integer, String>(
					nodes, "World");
			assert (mtask1.msgs.length == 1 && mtask1.msgs[0].equals("World"));
		}
	}
}
