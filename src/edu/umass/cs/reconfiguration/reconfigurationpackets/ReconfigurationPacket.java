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
package edu.umass.cs.reconfiguration.reconfigurationpackets;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.protocoltask.json.ProtocolPacket;
import edu.umass.cs.reconfiguration.Reconfigurator;
import edu.umass.cs.utils.IntegerPacketTypeMap;

/**
 * @author V. Arun
 * @param <NodeIDType> 
 */
@SuppressWarnings("javadoc")
public abstract class ReconfigurationPacket<NodeIDType> extends ProtocolPacket<NodeIDType, ReconfigurationPacket.PacketType> { 

	/**
	 * Reconfiguration packet type JSON key.
	 */
	public static final String PACKET_TYPE = JSONPacket.PACKET_TYPE;
	/**
	 * Used for reflection for invoking packet handling methods.
	 */
	public static final String HANDLER_METHOD_PREFIX = "handle";

	/********************************* End of ReconfigurationpacketType ***********************/
	public enum PacketType implements IntegerPacketType{

		/**
		 * A typical sequence of events is as follows. Active replicas regularly
		 * send DEMAND_REPORTs to reconfigurators. Upon the receipt of some (but
		 * far from all) demand report, a reconfigurator might decide to
		 * initiate a reconfiguration that proceeds as follows:
		 * 
		 * DEMAND_REPORT: coordinate "reconfiguration intent" RC_RECORD_REQUEST
		 * with other reconfigurators.
		 * 
		 * RC_RECORD_REQUEST (RECONFIGURATION_INTENT) : send STOP_EPOCH to all
		 * active replicas.
		 * 
		 * STOP_EPOCH: coordinate STOP_EPOCH with other active replicas.
		 * 
		 * PaxosPacket.DECISION(STOP_EPOCH) : send ACK_STOP_EPOCH to
		 * reconfigurators.
		 * 
		 * ACK_STOP_EPOCH : send START_EPOCH to next epoch active replicas.
		 * 
		 * START_EPOCH : send REQUEST_EPOCH_FINAL_STATE to previous epoch active
		 * replicas.
		 * 
		 * REQUEST_EPOCH_FINAL_STATE : send EPOCH_FINAL_STATE
		 * 
		 * EPOCH_FINAL_STATE : create new epoch group locally and send
		 * ACK_START_EPOCH to initiating reconfigurator.
		 * 
		 * ACK_START_EPOCH : upon receiving a majority of these from the new
		 * epoch, send DROP_EPOCH_FINAL_STATE to old epoch active replicas, and
		 * and coordinate RC_RECORD_REQUEST marking "reconfiguration complete"
		 * with other reconfigurators (although the reconfiguration is not
		 * completely complete yet).
		 * 
		 * RC_RECORD_REQUEST (RECONFIGURATION_COMPLETE) : mark the reconfigured
		 * name as READY to receive client requests.
		 * 
		 * DROP_EPOCH_FINAL_STATE : drop old, stopped epoch's final state and
		 * send ACK_DROP_EPOCH_FINAL_STATE to reconfigurator.
		 * 
		 * ACK_DROP_EPOCH_FINAL_STATE : wait until all old epoch active replicas
		 * have dropped their state, marking the final completion of the
		 * reconfiguration.
		 * 
		 */

		// reconfigurator -> active_replica
		STOP_EPOCH(224), // : coordinate stop with other actives
		START_EPOCH(226), // : request previous epoch final state
		DROP_EPOCH_FINAL_STATE(227), // : drop final state locally

		// active_replica -> active_replica
		REQUEST_EPOCH_FINAL_STATE(228), // : send epoch final state
		EPOCH_FINAL_STATE(229), // : record locally and ack to reconfigurator

		// active_replica -> reconfigurator
		DEMAND_REPORT(230), // : send stop_epoch if reconfiguration needed
		ACK_STOP_EPOCH(231), // : record locally
		ACK_START_EPOCH(232), // : record locally
		ACK_DROP_EPOCH_FINAL_STATE(233), // : record locally

		// app_client -> reconfigurator
		CREATE_SERVICE_NAME(234), // : initiate create
		DELETE_SERVICE_NAME(235), // : initiate delete
		REQUEST_ACTIVE_REPLICAS(236), // : send current active replicas
		
		// active_replica -> app_client
		ACTIVE_REPLICA_ERROR (237), 
		
		// reconfigurator -> reconfigurator
		RC_RECORD_REQUEST(238),
		
		// admin -> reconfigurator
		RECONFIGURE_RC_NODE_CONFIG (239);
		;

		private final int number;

		PacketType(int t) {this.number = t;}
		public int getInt() {return number;}

		public static final IntegerPacketTypeMap<PacketType> intToType = 
				new IntegerPacketTypeMap<PacketType>(PacketType.values());
	}
	
	public static final ReconfigurationPacket.PacketType[] clientPacketTypes = {PacketType.CREATE_SERVICE_NAME,
		PacketType.DELETE_SERVICE_NAME, PacketType.REQUEST_ACTIVE_REPLICAS, PacketType.ACTIVE_REPLICA_ERROR
	};
	
	
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
		typeMap.put(ReconfigurationPacket.PacketType.ACK_DROP_EPOCH_FINAL_STATE, AckDropEpochFinalState.class); 

		typeMap.put(ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME, CreateServiceName.class); 
		typeMap.put(ReconfigurationPacket.PacketType.DELETE_SERVICE_NAME, DeleteServiceName.class); 
		typeMap.put(ReconfigurationPacket.PacketType.REQUEST_ACTIVE_REPLICAS, RequestActiveReplicas.class); 

		typeMap.put(ReconfigurationPacket.PacketType.ACTIVE_REPLICA_ERROR, ActiveReplicaError.class); 

		typeMap.put(ReconfigurationPacket.PacketType.RC_RECORD_REQUEST, RCRecordRequest.class); 

		typeMap.put(ReconfigurationPacket.PacketType.RECONFIGURE_RC_NODE_CONFIG, ReconfigureRCNodeConfig.class); 

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

	public ReconfigurationPacket(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
		super(json, unstringer);
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
	
	public ReconfigurationPacket.PacketType getReconfigurationPacketType() {
		return this.type;
	}
	
	public static final boolean isReconfigurationPacket(JSONObject json) throws JSONException {
		return getReconfigurationPacketType(json)!=null;
	}

	public static final ReconfigurationPacket.PacketType getReconfigurationPacketType(JSONObject json) throws JSONException{
		if(json.has(ReconfigurationPacket.PACKET_TYPE)) 
			return ReconfigurationPacket.PacketType.intToType.get(json.getInt(PACKET_TYPE));
		else return null;		
	}
	public static final String getPacketTypeClassName(ReconfigurationPacket.PacketType type) {
		return typeMap.get(type)!=null ? typeMap.get(type).getSimpleName() : null;
	}
	public static final Class<?> getPacketTypeClass(ReconfigurationPacket.PacketType type) {
		return typeMap.get(type)!=null ? typeMap.get(type) : null;
	}

	public static BasicReconfigurationPacket<?> getReconfigurationPacket(JSONObject json, 
		Map<ReconfigurationPacket.PacketType,Class<?>> typeMap, Stringifiable<?> unstringer) throws JSONException {
		BasicReconfigurationPacket<?> rcPacket = null;
		ReconfigurationPacket.PacketType rcType = null;
		String packetClassesPackagePrefix = "edu.umass.cs.reconfiguration.reconfigurationpackets.";
		try {
			rcType = ReconfigurationPacket.PacketType.intToType.get(JSONPacket
					.getPacketType(json));
			if (rcType != null && getPacketTypeClassName(rcType) != null) {
				rcPacket = (BasicReconfigurationPacket<?>) (Class.forName(
						packetClassesPackagePrefix
								+ getPacketTypeClassName(rcType))
						.getConstructor(JSONObject.class, Stringifiable.class)
						.newInstance(json, unstringer));
			}
		} catch (NoSuchMethodException nsme) {
			nsme.printStackTrace();
		} catch (InvocationTargetException ite) {
			ite.printStackTrace();
		} catch (IllegalAccessException iae) {
			iae.printStackTrace();
		} catch (ClassNotFoundException cnfe) {
			Reconfigurator.getLogger().info(
					"Class " + packetClassesPackagePrefix
							+ getPacketTypeClassName(rcType) + " not found");
			cnfe.printStackTrace();
		} catch (InstantiationException ie) {
			ie.printStackTrace();
		} finally {
			if (ReconfigurationPacket.PacketType.intToType.get(JSONPacket
					.getPacketType(json)) == null)
				System.err.println("No reconfiguration packet type found in: " + json);
		}

		return rcPacket;
	}
	public static BasicReconfigurationPacket<?> getReconfigurationPacket(JSONObject json, Stringifiable<?> unstringer) throws JSONException {
		return getReconfigurationPacket(json, typeMap, unstringer);
	}
	public static BasicReconfigurationPacket<?> getReconfigurationPacket(Request request, Stringifiable<?> unstringer) throws JSONException {
		if(request instanceof BasicReconfigurationPacket<?>) return (BasicReconfigurationPacket<?>)request;
		return getReconfigurationPacket(new JSONObject(request.toString()), typeMap, unstringer);
	}

	/* ************************ Start of assertion methods **************************************************/ 
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
				" does not exist in " + target.getSimpleName();
		try {
			//System.out.println(type + " : " + packetName + " : " + handlerMethodPrefix+packetName);
			if(packetName!=null)
				assert (target.getMethod(handlerMethodPrefix + packetName,
						getPacketTypeClass(type), ProtocolTask[].class) != null) : errMsg ;
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

	static void main(String[] args) {
		System.out.println(ReconfigurationPacket.PacketType.intToType.get(225));
	}
}
