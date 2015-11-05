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

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.SampleNodeConfig;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.TESTProtocolTaskConfig;
import edu.umass.cs.protocoltask.ThresholdProtocolTask;
import edu.umass.cs.protocoltask.examples.pingpong.PingPongProtocolTask;
import edu.umass.cs.protocoltask.examples.thresholdfetch.MajorityFetchProtocolTask;

/**
 * @author V. Arun
 */
@SuppressWarnings("javadoc")
public class ExampleNode {
	private final int myID;
	private final Set<Integer> nodeIDs;
	private final JSONNIOTransport<Integer> niot;
	private final ProtocolExecutor<Integer, PingPongPacket.PacketType, String> protocolExecutor;
	
	private Logger log = Logger.getLogger(getClass().getName());

	ExampleNode(int id, NodeConfig<Integer> nc) throws IOException {
		// Setting my ID and NIO with packet demultiplexer
		this.myID = id;
		this.nodeIDs = (nc.getNodeIDs());
		this.niot =
				new JSONNIOTransport<Integer>(id, nc, new ExamplePacketDemultiplexer(
						this), true);

		// protocol executor
		this.protocolExecutor =
				new ProtocolExecutor<Integer, PingPongPacket.PacketType, String>(
						new JSONMessenger<Integer>(niot));
		
		// Create and register local services (i.e., another level of demultiplexing)
		this.protocolExecutor.register(PingPongPacket.PacketType.TEST_PING,
				new PingPongServer(this.myID));
		log.info("Node " + myID + " inserted key=" +
				PingPongPacket.PacketType.TEST_PING);
	}

	public boolean handleIncoming(JSONObject msg) {
		try {
			switch (PingPongPacket.PacketType.intToType.get(JSONPacket.getPacketType(msg))) {
			case TEST_PONG:
			case TEST_PING:
				this.protocolExecutor.handleEvent(new PingPongPacket(msg));
				break;
				default:
					break;
			}
		} catch (JSONException je) {
			je.printStackTrace();
		}
		return true;
	}

	protected int getMyID() {
		return this.myID;
	}

	protected void stop() {
		this.niot.stop();
		this.protocolExecutor.stop();
	}

	/*
	 * Will run a finite number of ping pongs and stop when it receives
	 * the specified number of pongs from any node in the specified set
	 * of nodes.
	 */
	protected PingPongProtocolTask finitePingPong() {
		PingPongProtocolTask task =
				new PingPongProtocolTask(this.myID, this.nodeIDs,
						PingPongProtocolTask.MAX_PINGS);
		log.info("Node" + myID + " spawning finite pingpong protocol task");
		this.protocolExecutor.spawn(task);
		return task;
	}

	/*
	 * Will ensure that a message gets sent reliably to at least one node
	 * in the specified set of nodes. Very similar to finite ping pongs
	 * above.
	 */
	protected PingPongProtocolTask reliableSend(int id) {
		TreeSet<Integer> nodes = new TreeSet<Integer>();
		nodes.add(id);
		PingPongProtocolTask task =
				new PingPongProtocolTask(this.myID, nodes, 1);
		log.info("Node" + myID + " spawning reliableSend protocol task");
		this.protocolExecutor.spawn(task);
		return task;
	}

	/*
	 * Will ensure that a message gets reliably sent to a threshold number (majority)
	 * of nodes in the specified set. ThresholdProtocolTask will automatically ensure
	 * that the message is retransmitted only to nodes that have not yet acknowledged
	 * it.
	 */
	protected ThresholdProtocolTask<Integer, PingPongPacket.PacketType, String> thresholdFetch() {
		MajorityFetchProtocolTask task =
				new MajorityFetchProtocolTask(this.myID, this.nodeIDs);
		log.info("Node" + myID + " spawning threshold protocol task");
		this.protocolExecutor.schedule(task, 1000);
		return task;
	}

	 static void main(String[] args) {
		int startID = 100;
		int numNodes = 2;
		SampleNodeConfig<Integer> snc = new SampleNodeConfig<Integer>();
		for (int i = startID; i < startID + numNodes; i++) {
			snc.addLocal(i);
		}
		ExampleNode[] nodes = new ExampleNode[numNodes];
		try {
			for (int i = 0; i < numNodes; i++) {
				nodes[i] = new ExampleNode(startID + i, snc);
			}
			System.out.print("Testing finite ping pong with " +
					PingPongProtocolTask.MAX_PINGS + " pings...");
			TESTProtocolTaskConfig.setDrop(false);
			nodes[0].finitePingPong();
			while (!nodes[0].protocolExecutor.isEmpty())
				;
			System.out.println(" done.");

			System.out.println("Testing reliable send (also using PingPongProtocolTask)...");
			nodes[0].reliableSend(nodes[1].getMyID());
			while (!nodes[0].protocolExecutor.isEmpty())
				;
			System.out.println(" done.");

			System.out.println("Testing reliable threshold fetch using MajorityFetchProtocolTask and PingPongServer...");
			nodes[0].thresholdFetch();
			while (!nodes[0].protocolExecutor.isEmpty())
				;
			System.out.println(" done.");

			System.out.println("SUCCESS: Tested finite ping pong, reliable send, and threshold fetch protocol tasks.");

			for (int i = 0; i < numNodes; i++) {
				assert(nodes[i].protocolExecutor.isEmpty());
				nodes[i].stop();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
