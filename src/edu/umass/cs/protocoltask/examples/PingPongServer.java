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
package edu.umass.cs.protocoltask.examples;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import edu.umass.cs.nio.nioutils.MessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.protocoltask.TESTProtocolTaskConfig;

/**
 * @author V. Arun
 * 
 */
@SuppressWarnings("javadoc")
public class PingPongServer implements ProtocolTask<Integer, PingPongPacket.PacketType, String> {

	private final String key;

	protected final Integer myID;
	private static PingPongPacket.PacketType[] types = {PingPongPacket.PacketType.TEST_PING};

	private Logger log = Logger.getLogger(getClass().getName());

	public PingPongServer(int id) {
		this.myID = id;
		this.key = refreshKey();
	}

	/*************************** Start of overridden methods *****************************************/
	@Override
	public String getKey() {
		return this.key;
	}

	//@Override
	public String refreshKey() {
		return (this.myID.toString() +
				(int) (Math.random() * Integer.MAX_VALUE));
	}

	@Override
	public MessagingTask[] handleEvent(
			ProtocolEvent<PingPongPacket.PacketType, String> event,
			ProtocolTask<Integer, PingPongPacket.PacketType, String>[] ptasks) {

		PingPongPacket ppp = ((PingPongPacket) event.getMessage());
		MessagingTask mtask = null;
		switch (ppp.getType()) {
		case TEST_PING:
			mtask = handlePingPong(ppp);
			break;
		default:
			throw new RuntimeException("Unrecognizable message");
		}
		return mtask != null ? mtask.toArray() : null;
	}

	@Override
	public MessagingTask[] start() {
		return null;
	}

	/*************************** End of overridden methods *****************************************/

	/*************************** Private or testing methods below *********************************/
	private MessagingTask handlePingPong(PingPongPacket ppp) {
		return handlePing(ppp);
	}

	private MessagingTask handlePing(PingPongPacket ping) {
		int sender = ping.flip(this.myID);
		if (TESTProtocolTaskConfig.shouldDrop(ping.getCounter()))
			return null;
		log.info("Node" + myID + " pingpong server ponging to " + sender +
				": " + ping);
		return new MessagingTask(Integer.valueOf(sender), ping);
	}

	@Override
	public Set<PingPongPacket.PacketType> getEventTypes() {
		return new HashSet<PingPongPacket.PacketType>(Arrays.asList(types)); //types;
	}
}
