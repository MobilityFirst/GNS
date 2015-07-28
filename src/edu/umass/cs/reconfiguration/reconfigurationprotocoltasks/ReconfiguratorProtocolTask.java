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

import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.reconfiguration.Reconfigurator;
import edu.umass.cs.reconfiguration.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;

/**
@author V. Arun
 * @param <NodeIDType> 
 */
public class ReconfiguratorProtocolTask<NodeIDType> implements
ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String> {

	private static final String HANDLER_METHOD_PREFIX = ReconfigurationPacket.HANDLER_METHOD_PREFIX; // could be any String as scope is local
	private static final ReconfigurationPacket.PacketType[] localTypes = {
		ReconfigurationPacket.PacketType.DEMAND_REPORT,
		ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME,
		ReconfigurationPacket.PacketType.DELETE_SERVICE_NAME,
		ReconfigurationPacket.PacketType.REQUEST_ACTIVE_REPLICAS,
		ReconfigurationPacket.PacketType.RC_RECORD_REQUEST,
		ReconfigurationPacket.PacketType.RECONFIGURE_RC_NODE_CONFIG,
	};
	private static final ReconfigurationPacket.PacketType[] types = 
			ReconfigurationPacket.concatenate(localTypes, 
		WaitAckStopEpoch.types, 
		WaitAckStartEpoch.types, 
		WaitAckDropEpoch.types
		);

	static { // all but DEMAND_REPORT are handled by temporary protocol tasks
		ReconfigurationPacket.assertPacketTypeChecks(localTypes, Reconfigurator.class, HANDLER_METHOD_PREFIX); 
	}

	private final String key;
	private final NodeIDType myID;
	private final Object reconfigurator;

	/**
	 * @param id
	 * @param reconfigurator
	 */
	public ReconfiguratorProtocolTask(NodeIDType id, Reconfigurator<NodeIDType> reconfigurator) {
		this.myID = id;
		this.reconfigurator = reconfigurator;
		this.key =  refreshKey();
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
		return ((this.myID.toString() + (int) (Math.random() * Integer.MAX_VALUE)));
	}

	@Override
	public Set<ReconfigurationPacket.PacketType> getEventTypes() {
		return new HashSet<ReconfigurationPacket.PacketType>(Arrays.asList(types));
	}
	/**
	 * @return Default reconfiguration packet types handled, i.e., not counting temporary
	 * protocol tasks.
	 */
	public Set<ReconfigurationPacket.PacketType> getDefaultTypes() {
		return new HashSet<ReconfigurationPacket.PacketType>(Arrays.asList(localTypes));
	}

	@SuppressWarnings("unchecked")
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleEvent(
		ProtocolEvent<ReconfigurationPacket.PacketType, String> event,
		ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {

		return (GenericMessagingTask<NodeIDType, ?>[])(autoInvokeMethod(this.reconfigurator, event, ptasks));
	}
	
	/**
	 * @param target
	 * @param event
	 * @param ptasks
	 * @return Object returned by the packet handler.
	 */
	public static Object autoInvokeMethod(Object target, ProtocolEvent<ReconfigurationPacket.PacketType, String> event,
			ProtocolTask<?, ReconfigurationPacket.PacketType, String>[] ptasks) {
		ReconfigurationPacket.PacketType type = event.getType();
		try {
			return target.getClass().getMethod(HANDLER_METHOD_PREFIX+
				ReconfigurationPacket.getPacketTypeClassName(type), ReconfigurationPacket.getPacketTypeClass(type), 
				ProtocolTask[].class).invoke(target, 
					(BasicReconfigurationPacket<?>)event, ptasks);
		} catch(NoSuchMethodException nsme) {
			nsme.printStackTrace();
		} catch(InvocationTargetException ite) {
			ite.printStackTrace();
		} catch(IllegalAccessException iae) {
			iae.printStackTrace();
		}
		return null;
	}

}
