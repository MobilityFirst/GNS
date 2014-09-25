package edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.nio.JSONPacket;
import edu.umass.cs.gns.protocoltask.ProtocolEvent;
import edu.umass.cs.gns.protocoltask.ProtocolTask;
import edu.umass.cs.gns.protocoltask.json.ProtocolPacket;
import edu.umass.cs.gns.util.IntegerPacketTypeMap;

/**
 * @author V. Arun
 */
public abstract class ReconfigurationPacket<NodeIDType> extends ProtocolPacket<NodeIDType, ReconfigurationPacket.PacketType> { 

	public static final String PACKET_TYPE = JSONPacket.PACKET_TYPE;
	public static final String HANDLER_METHOD_PREFIX = "handle";

	/********************************* End of ReconfigurationpacketType ***********************/
	public enum PacketType implements IntegerPacketType{

		// events at active replica : actions at active replica
		STOP_EPOCH (224), // : propose stop
		START_EPOCH (226), // : request previous epoch final state
		REQUEST_EPOCH_FINAL_STATE (227), // : send epoch final state
		EPOCH_FINAL_STATE (228), // : record locally
		DROP_EPOCH_FINAL_STATE (229), // : drop final state

		// events at reconfigurator
		DEMAND_REPORT (230), // : starting point for reconfiguration, send stop_epoch if needed
		ACK_STOP_EPOCH (231), // : record locally
		ACK_START_EPOCH (232), // : record locally
		ACK_EPOCH_FINAL_STATE (233), // : record locally; if majority, send drop_epoch_final_state
		ACK_DROP_EPOCH_FINAL_STATE (234),
		CREATE_SERVICE_NAME (235),
		DELETE_SERVICE_NAME (236),
		;

		private final int number;

		PacketType(int t) {this.number = t;}
		public int getInt() {return number;}

		public static final IntegerPacketTypeMap<PacketType> intToType = 
				new IntegerPacketTypeMap<PacketType>(PacketType.values());
	}
	/********************************* End of ReconfigurationpacketType ***********************/

	/**************************** Start of ReconfigurationpacketType class map **************/
	private static final HashMap<ReconfigurationPacket.PacketType, Class<?>> typeMap = 
			new HashMap<ReconfigurationPacket.PacketType, Class<?>>();
	static { 
		/* This map prevents the need for laborious switch/case sequences as it automatically
		 * handles both json-to-ReconfigurationPacket conversion and invocation of the 
		 * corresponding handler method. We have to rely on reflection for both and the 
		 * cost of the former seems to be the bottleneck as it adds ~25us per conversion,
		 * but it seems not problematic for now. 
		 */
		typeMap.put(ReconfigurationPacket.PacketType.STOP_EPOCH, StopEpoch.class);
		typeMap.put(ReconfigurationPacket.PacketType.START_EPOCH, StartEpoch.class);
		typeMap.put(ReconfigurationPacket.PacketType.REQUEST_EPOCH_FINAL_STATE, RequestEpochFinalState.class);
		typeMap.put(ReconfigurationPacket.PacketType.EPOCH_FINAL_STATE, EpochFinalState.class);
		typeMap.put(ReconfigurationPacket.PacketType.DROP_EPOCH_FINAL_STATE, DropEpochFinalState.class);

		typeMap.put(ReconfigurationPacket.PacketType.DEMAND_REPORT, DemandReport.class);
		typeMap.put(ReconfigurationPacket.PacketType.ACK_STOP_EPOCH, AckStopEpoch.class);
		typeMap.put(ReconfigurationPacket.PacketType.ACK_START_EPOCH, AckStartEpoch.class);
		typeMap.put(ReconfigurationPacket.PacketType.ACK_EPOCH_FINAL_STATE, EpochFinalState.class);
		typeMap.put(ReconfigurationPacket.PacketType.ACK_DROP_EPOCH_FINAL_STATE, AckDropEpochFinalState.class); 
		typeMap.put(ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME, CreateServiceName.class); 
		typeMap.put(ReconfigurationPacket.PacketType.DELETE_SERVICE_NAME, DeleteServiceName.class); 

		for(ReconfigurationPacket.PacketType type : ReconfigurationPacket.PacketType.intToType.values()) {
			assert(getPacketTypeClassName(type)!=null) : type;
		}
	}
	/**************************** End of ReconfigurationpacketType class map **************/


	// FIXME: probably should be removed
	protected ReconfigurationPacket(NodeIDType initiator) {
		super(initiator);
		//this.setType(t);
	}

	public ReconfigurationPacket(JSONObject json) throws JSONException {
		super(json);
		this.setType(getPacketType(json));
	}	

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		return json;
	}

	@Override
	public Object getMessage() {
		return this;
	}

	@Override
	public PacketType getPacketType(JSONObject json)
			throws JSONException {
		return getReconfigurationPacketType(json);
	}

	@Override
	public void putPacketType(JSONObject json, PacketType type)
			throws JSONException {
		json.put(PACKET_TYPE, type.getInt());
	}

	public String toString() {
		try {
			return this.toJSONObject().toString();
		} catch(JSONException je) {
			je.printStackTrace();
		}
		return null;
	}

	public static final ReconfigurationPacket.PacketType getReconfigurationPacketType(JSONObject json) throws JSONException{
		if(json.has(ReconfigurationPacket.PACKET_TYPE)) 
			return ReconfigurationPacket.PacketType.intToType.get(json.getInt(PACKET_TYPE));
		else return null;		
	}
	public static final String getPacketTypeClassName(ReconfigurationPacket.PacketType type) {
		return typeMap.get(type)!=null ? typeMap.get(type).getSimpleName() : null;
	}

	public static BasicReconfigurationPacket<?> getReconfigurationPacket(JSONObject json, 
		Map<ReconfigurationPacket.PacketType,Class<?>> typeMap) throws JSONException {
		BasicReconfigurationPacket<?> rcPacket = null;
		try {
			ReconfigurationPacket.PacketType rcType = 
					ReconfigurationPacket.PacketType.intToType.get(JSONPacket.getPacketType(json)); 
			if(rcType!=null && getPacketTypeClassName(rcType)!=null) {
				rcPacket = (BasicReconfigurationPacket<?>)(Class.forName(
					"edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets." + 
							getPacketTypeClassName(rcType)).getConstructor(JSONObject.class).newInstance(json));
			}
		}
		catch(NoSuchMethodException nsme) {nsme.printStackTrace();} 
		catch(InvocationTargetException ite) {ite.printStackTrace();} 
		catch(IllegalAccessException iae) {iae.printStackTrace();} 
		catch(ClassNotFoundException cnfe) {cnfe.printStackTrace();}
		catch(InstantiationException ie) {ie.printStackTrace();}

		return rcPacket;
	}
	public static BasicReconfigurationPacket<?> getReconfigurationPacket(JSONObject json) throws JSONException {
		return getReconfigurationPacket(json, typeMap);
	}

	/************************* Start of assertion methods **************************************************/ 
	/* The assertion methods below are just convenience methods to let protocoltasks 
	 * assert that they have set up handlers for all packet types for which they
	 * are responsible.
	 */
	public static void assertPacketTypeChecks(ReconfigurationPacket.PacketType[] types, Class<?> target, String handlerMethodPrefix) {
		for(ReconfigurationPacket.PacketType type : types) {
			assertPacketTypeChecks(type, getPacketTypeClassName(type), target, handlerMethodPrefix);			
		}
	}
	public static void assertPacketTypeChecks(Map<ReconfigurationPacket.PacketType,Class<?>> typeMap, Class<?> target, String handlerMethodPrefix) {
		// Assertions ensure that method name changes do not break code.
		for(ReconfigurationPacket.PacketType type : typeMap.keySet()) {
			assertPacketTypeChecks(type, getPacketTypeClassName(type), target, handlerMethodPrefix);
		}
	}
	public static void assertPacketTypeChecks(ReconfigurationPacket.PacketType type, String packetName, 
			Class<?> target, String handlerMethodPrefix) {
		String errMsg = "Method " + handlerMethodPrefix+packetName +
				" does not exist in ReconfiguratorProtocolTask";
		try {
			System.out.println(type + " : " + packetName);
			if(packetName!=null)
				assert(target.getMethod(handlerMethodPrefix+packetName, 
					ProtocolEvent.class, ProtocolTask[].class)!=null) : 
						errMsg;
		} catch(NoSuchMethodException nsme) {
			System.err.println(errMsg);
			nsme.printStackTrace();
		}
	}
	public static void assertPacketTypeChecks(Map<ReconfigurationPacket.PacketType,Class<?>> typeMap, Class<?> target) {
		assertPacketTypeChecks(typeMap, target, HANDLER_METHOD_PREFIX);
	}
	
	public static ReconfigurationPacket.PacketType[] concatenate(ReconfigurationPacket.PacketType[]... types) {
		int size=0;
		for(ReconfigurationPacket.PacketType[] tarray : types) size += tarray.length;
		ReconfigurationPacket.PacketType[] allTypes = new ReconfigurationPacket.PacketType[size];
		int i=0;
		for(ReconfigurationPacket.PacketType[] tarray : types) {
			for(ReconfigurationPacket.PacketType type : tarray) {
				allTypes[i++] = type;
			}
		}
		return allTypes;
	}
	/************************* End of assertion methods **************************************************/ 

	public static void main(String[] args) {
		System.out.println(ReconfigurationPacket.PacketType.intToType.get(225));
	}
}
