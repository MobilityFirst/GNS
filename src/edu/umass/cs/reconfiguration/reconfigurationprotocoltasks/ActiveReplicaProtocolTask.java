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
package edu.umass.cs.reconfiguration.reconfigurationprotocoltasks;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.reconfiguration.ActiveReplica;
import edu.umass.cs.reconfiguration.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;

/**
@author V. Arun
 * @param <NodeIDType> 
 */
public class ActiveReplicaProtocolTask<NodeIDType> implements
ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String> {

	private static final String HANDLER_METHOD_PREFIX = ReconfigurationPacket.HANDLER_METHOD_PREFIX; // could be any String as scope is local
	private static final ReconfigurationPacket.PacketType[] defaultTypes = {
		ReconfigurationPacket.PacketType.STOP_EPOCH,
		ReconfigurationPacket.PacketType.START_EPOCH,
		ReconfigurationPacket.PacketType.REQUEST_EPOCH_FINAL_STATE,
		ReconfigurationPacket.PacketType.DROP_EPOCH_FINAL_STATE
	};
	private static final ReconfigurationPacket.PacketType[] types = ReconfigurationPacket.concatenate(defaultTypes,
		WaitEpochFinalState.types);
	static { 
		ReconfigurationPacket.assertPacketTypeChecks(defaultTypes, ActiveReplica.class, HANDLER_METHOD_PREFIX);
	}
	
	private final String key;
	private final NodeIDType myID;
	private final Stringifiable<NodeIDType> unstringer;
	private final ActiveReplica<NodeIDType> activeReplica;

	/**
	 * @param id
	 * @param unstringer 
	 * @param ar
	 */
	public ActiveReplicaProtocolTask(NodeIDType id, Stringifiable<NodeIDType> unstringer, ActiveReplica<NodeIDType> ar) {
		this.myID = id;
		this.activeReplica = ar;
		this.unstringer = unstringer;
		this.key = refreshKey();
	}

	@Override
	public String getKey() {
		return this.key;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] start() {
		// do nothing in particular to bootstrap
		return null;
	}

	/**
	 * @return The refreshed key.
	 */
	public String refreshKey() {
		return (
				(this.myID.toString() + (int) (Math.random() * Integer.MAX_VALUE)));
	}


	@Override
	public Set<ReconfigurationPacket.PacketType> getEventTypes() {
		Set<ReconfigurationPacket.PacketType> types = new HashSet<ReconfigurationPacket.PacketType>(
				Arrays.asList(ActiveReplicaProtocolTask.types));
		return types;
	}

	/**
	 * @return Default packet types handled, i.e., not counting temporary
	 *         protocol tasks.
	 */
	public Set<ReconfigurationPacket.PacketType> getDefaultTypes() {
		return new HashSet<ReconfigurationPacket.PacketType>(Arrays.asList(defaultTypes));
	}

	@SuppressWarnings("unchecked")
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleEvent(
		ProtocolEvent<ReconfigurationPacket.PacketType, String> event,
		ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {

		ReconfigurationPacket.PacketType type = event.getType();
		Object returnValue = null;
		try {
			if (ReconfigurationPacket.getPacketTypeClass(type) != null)
				returnValue = this.activeReplica
						.getClass()
						.getMethod(
								HANDLER_METHOD_PREFIX
										+ ReconfigurationPacket
												.getPacketTypeClassName(type),
								ReconfigurationPacket.getPacketTypeClass(type),
								ProtocolTask[].class)
						.invoke(this.activeReplica,
								(BasicReconfigurationPacket<?>) event, ptasks);
			else
				assert (false);
		} catch(NoSuchMethodException nsme) {
			nsme.printStackTrace();
		} catch(InvocationTargetException ite) {
			ite.printStackTrace();
		} catch(IllegalAccessException iae) {
			iae.printStackTrace();
		}
		return (GenericMessagingTask<NodeIDType, ?>[])returnValue;
	}

	/**
	 * @param json
	 * @return BasicReconfigurationPacket generated via reflection from JSON.
	 * @throws JSONException
	 */
	@SuppressWarnings("unchecked")
	public BasicReconfigurationPacket<NodeIDType> getReconfigurationPacket(JSONObject json) throws JSONException {
		return (BasicReconfigurationPacket<NodeIDType>)ReconfigurationPacket.getReconfigurationPacket(json, this.unstringer);
	}
}

