package edu.umass.cs.reconfiguration.json;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.reconfiguration.Reconfigurator;
import edu.umass.cs.reconfiguration.json.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;

/**
@author V. Arun
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

	private String key = null;
	private final NodeIDType myID;
	private final Object reconfigurator;

	public ReconfiguratorProtocolTask(NodeIDType id, Reconfigurator<NodeIDType> reconfigurator) {
		this.myID = id;
		this.reconfigurator = reconfigurator;
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
		return new HashSet<ReconfigurationPacket.PacketType>(Arrays.asList(types));
	}
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

	public static void main(String[] args) {
	}
}
