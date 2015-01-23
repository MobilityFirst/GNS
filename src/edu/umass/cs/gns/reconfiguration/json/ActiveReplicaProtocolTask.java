package edu.umass.cs.gns.reconfiguration.json;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.protocoltask.ProtocolEvent;
import edu.umass.cs.gns.protocoltask.ProtocolTask;
import edu.umass.cs.gns.reconfiguration.ActiveReplica;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;

/**
@author V. Arun
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
	
	private String key = null;
	private final NodeIDType myID;
	private final ActiveReplica<NodeIDType> activeReplica;

	public ActiveReplicaProtocolTask(NodeIDType id, ActiveReplica<NodeIDType> ar) {
		this.myID = id;
		this.activeReplica = ar;
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

	@Override
	public String refreshKey() {
		return (this.key =
				(this.myID.toString() + (int) (Math.random() * Integer.MAX_VALUE)));
	}


	@Override
	public Set<ReconfigurationPacket.PacketType> getEventTypes() {
		Set<ReconfigurationPacket.PacketType> types = new HashSet<ReconfigurationPacket.PacketType>(
				Arrays.asList(ActiveReplicaProtocolTask.types));
		return types;
	}
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

	@SuppressWarnings("unchecked")
	public BasicReconfigurationPacket<NodeIDType> getReconfigurationPacket(JSONObject json) throws JSONException {
		return (BasicReconfigurationPacket<NodeIDType>)ReconfigurationPacket.getReconfigurationPacket(json, this.activeReplica.getUnstringer());
	}

	public static void main(String[] args) {
	}
}
