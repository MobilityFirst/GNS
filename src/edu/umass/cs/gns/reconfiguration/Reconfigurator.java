package edu.umass.cs.gns.reconfiguration;

import java.util.Set;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.nio.InterfacePacketDemultiplexer;
import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.nio.JSONPacket;
import edu.umass.cs.gns.nio.NIOTransport;
import edu.umass.cs.gns.protocoltask.ProtocolEvent;
import edu.umass.cs.gns.protocoltask.ProtocolExecutor;
import edu.umass.cs.gns.protocoltask.ProtocolTask;
import edu.umass.cs.gns.reconfiguration.json.ReconfiguratorProtocolTask;
import edu.umass.cs.gns.reconfiguration.json.WaitAckStartEpoch;
import edu.umass.cs.gns.reconfiguration.json.WaitAckStopEpoch;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.DemandReport;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.StartEpoch;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ReconfigurationRecord;

/**
@author V. Arun
 */
public class Reconfigurator<NodeIDType> implements InterfacePacketDemultiplexer {

	private final JSONMessenger<NodeIDType> messenger;
	private final ProtocolExecutor<NodeIDType, ReconfigurationPacket.PacketType, String> protocolExecutor;
	private final ReconfiguratorProtocolTask<NodeIDType> protocolTask;
	private final AbstractReconfiguratorDB<NodeIDType> DB;
	
	private static boolean DEBUG = true;
	public static final Logger log =
			NIOTransport.LOCAL_LOGGER ? Logger.getLogger(NIOTransport.class.getName())
					: GNS.getLogger();
	
	// Any id-based communication requires NodeConfig and Messenger
	public Reconfigurator(InterfaceReconfigurableNodeConfig<NodeIDType> nc, JSONMessenger<NodeIDType> m) {
		this.messenger = m;
		this.protocolExecutor = new ProtocolExecutor<NodeIDType, ReconfigurationPacket.PacketType, String>(messenger);
		this.protocolTask = new ReconfiguratorProtocolTask<NodeIDType>(getMyID(), this);
		this.protocolExecutor.register(this.protocolTask.getDefaultTypes(), this.protocolTask);
		this.DB = new DerbyReconfiguratorDB<NodeIDType>(this.messenger.getMyID(), nc);
	}
	
	@Override
	public boolean handleJSONObject(JSONObject jsonObject) {
		BasicReconfigurationPacket<NodeIDType> rcPacket = null;
		if(DEBUG) Reconfigurator.log.finest("Reconfigurator received " + jsonObject);
		try {
			// try handling as reconfiguration packet through protocol task 
			if((rcPacket = this.protocolTask.getReconfigurationPacket(jsonObject))!=null) {
				this.protocolExecutor.handleEvent(rcPacket);
			} else if(isExternalRequest(jsonObject)) {
				assert(false);
			}
		} catch(JSONException je) {
			je.printStackTrace();
		}
		return false; // neither reconfiguration packet nor app request
	}
	public Set<ReconfigurationPacket.PacketType> getPacketTypes() {
		return this.protocolTask.getEventTypes();
	}
	
	private boolean isExternalRequest(JSONObject json) throws JSONException {
		ReconfigurationPacket.PacketType rcType = 
				ReconfigurationPacket.getReconfigurationPacketType(json);
		return false;
	}

	/****************************** Start of protocol task handler methods *********************/
	public GenericMessagingTask<NodeIDType,?>[] handleDemandReport(
		ProtocolEvent<ReconfigurationPacket.PacketType, String> event,
		ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		System.out.println("RC"+getMyID() + " received " + event.getType() + " " + event.getMessage());
		/* Actions:
		 * - record and update stats for that name
		 * - initiate reconfiguration if needed
		 */
		@SuppressWarnings("unchecked")
		DemandReport<NodeIDType> report = (DemandReport<NodeIDType>)event;
		if(this.DB.updateStats(report)) {
			if(this.amIResponsible(report.getServiceName()) && 
					this.DB.setStateIfReady(report.getServiceName(), report.getEpochNumber(), 
						ReconfigurationRecord.RCStates.WAIT_ACK_STOP)) {
				ptasks[0] = new WaitAckStopEpoch<NodeIDType>(formStartEpoch(report), this.DB);
			}
		}
		return null; // startEpoch message is in ptasks[0].start()
	}
	
	public GenericMessagingTask<NodeIDType,?>[] handleCreateServiceName(
		ProtocolEvent<ReconfigurationPacket.PacketType, String> event,
		ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		/* Actions:
		 * - send start epoch with empty previous epoch information to default actives
		 */
		CreateServiceName create = (CreateServiceName)event;

		if(!amIResponsible(create.getServiceName())) return getForwardedRequest(create).toArray();
		// else 
		WaitAckStartEpoch<NodeIDType> startTask = new WaitAckStartEpoch<NodeIDType>(
				new StartEpoch<NodeIDType>(getMyID(), create.getServiceName(), 0, 
						this.DB.getDefaultActiveReplicas(create.getServiceName()), null), 
						this.DB, create);
		ptasks[0] = startTask;
		return null;
	}
	
	private GenericMessagingTask<NodeIDType,?> getForwardedRequest(CreateServiceName create) {
		NodeIDType id = this.DB.CH_RC.getReplicatedServers(create.getServiceName()).iterator().next();
		return new GenericMessagingTask<NodeIDType,CreateServiceName>(id, create);
	}
	
	public GenericMessagingTask<NodeIDType,?>[] handleDeleteServiceName(
		ProtocolEvent<ReconfigurationPacket.PacketType, String> event,
		ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		
		return null;
	}

	/****************************** End of protocol task handler methods *********************/

	/*********************** Private methods below **************************/
	
	/* FIXME: Only the first reconfigurator currently does anything useful.
	 * Need to systematically coordinate with other reconfigurators using 
	 * paxos.
	 */
	private boolean amIResponsible(String name) {
		return this.DB.amIResponsible(name);
	}
	private NodeIDType getMyID() {
		return this.messenger.getMyID();
	}
	private StartEpoch<NodeIDType> formStartEpoch(DemandReport<NodeIDType> report) {
		ReconfigurationRecord<NodeIDType> record = this.DB.getReconfigurationRecord(
			report.getServiceName(), report.getEpochNumber());
		Set<NodeIDType> curActives = record.getActiveReplicas(
			record.getName(), record.getEpoch());
		Set<NodeIDType> newActives = selectNewActives(record);
		return new StartEpoch<NodeIDType>(getMyID(), 
				record.getName(), record.getEpoch()+1, newActives, curActives);
	}
	private Set<NodeIDType> selectNewActives(ReconfigurationRecord<NodeIDType> record) {
		Set<NodeIDType> curActives = record.getActiveReplicas(
			record.getName(), record.getEpoch());
		Set<NodeIDType> newActives = curActives; // FIXME: simply returns same set
		return newActives;  
	}
}
