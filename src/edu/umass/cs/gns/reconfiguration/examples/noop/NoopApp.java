package edu.umass.cs.gns.reconfiguration.examples.noop;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurable;
import edu.umass.cs.gns.reconfiguration.InterfaceReplicable;
import edu.umass.cs.gns.reconfiguration.InterfaceRequest;
import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurableRequest;
import edu.umass.cs.gns.reconfiguration.RequestParseException;
import edu.umass.cs.gns.reconfiguration.examples.AppRequest;

/**
 * @author V. Arun
 */
public class NoopApp implements
InterfaceReplicable, InterfaceReconfigurable {

	private static final String DEFAULT_INIT_STATE = "";
	
	private class AppData {
		final String name;
		final int epoch;
		String state=DEFAULT_INIT_STATE;
		AppData(String name, int epoch, String state) {this.name=name; this.epoch=epoch; this.state=state;}
		void setState(String state) {this.state = state;}
		String getState() {return this.state;}
	}
	private final int myID;
	private final HashMap<String, AppData> appData = new HashMap<String,AppData>();
	private final HashMap<String, AppData> prevEpochFinal = new HashMap<String,AppData>();

	public NoopApp(int id) {
		this.myID = id;
	}

	@Override
	public boolean handleRequest(InterfaceRequest request, boolean doNotReplyToClient) {
		try {
			switch ((AppRequest.PacketType)(request.getRequestType())) {
			case DEFAULT_APP_REQUEST:
				return processRequest((NoopAppRequest)request);
			default:
				break;
			}
		} catch (RequestParseException rpe) {
			rpe.printStackTrace();
		}
		return false;
	}
	
	private boolean processRequest(NoopAppRequest request) {
		if(request.isStop()) return processStopRequest(request);
		AppData data = this.appData.get(request.getServiceName());
		if(data==null) {
			// create if non-existent name
			AppData prevData = this.prevEpochFinal.get(request.getServiceName());
			data = new AppData(request.getServiceName(), 
				(prevData!=null ? prevData.epoch+1 : 0), request.getValue()); 
			throw new RuntimeException("App"+myID+" has no record for "+ request.getServiceName());
		}
		assert(data!=null);
		data.setState(request.getValue());
		this.appData.put(request.getServiceName(), data);
		System.out.println("App"+myID+" wrote " + data.name+":"+data.epoch + " with state "+data.getState());
		return true;
	}
	private boolean processStopRequest(NoopAppRequest request) {
		AppData data = this.appData.remove(request.getServiceName());
		if(data==null) return false;
		this.prevEpochFinal.put(request.getServiceName(), data);
		System.out.println("App-"+myID+" stopped " + data.name+":"+getEpoch(request.getServiceName()) + " with state "+data.getState());
		return true;
	}

	@Override
	public InterfaceRequest getRequest(String stringified)
			throws RequestParseException {
		NoopAppRequest request = null;
		try {
			request = new NoopAppRequest(new JSONObject(stringified));
		} catch (JSONException je) {
			throw new RequestParseException(je);
		}
		return request;
	}

	private static AppRequest.PacketType[] types = {AppRequest.PacketType.DEFAULT_APP_REQUEST, AppRequest.PacketType.APP_COORDINATION};

	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return new HashSet<IntegerPacketType>(Arrays.asList(types));
	}

	@Override
	public boolean handleRequest(InterfaceRequest request) {
		return this.handleRequest(request, false);
	}

	@Override
	public String getState(String name, int epoch) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean updateState(String name, String state) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public InterfaceReconfigurableRequest getStopRequest(String name, int epoch) {
		return new NoopAppRequest(name, epoch, (int)(Math.random()*Integer.MAX_VALUE), "", AppRequest.PacketType.DEFAULT_APP_REQUEST, true);
	}

	@Override
	public String getFinalState(String name, int epoch) {
		AppData state = this.prevEpochFinal.get(name);
		if(state!=null && state.epoch==epoch) return state.getState();
		else return null;
	}

	@Override
	public void putInitialState(String name, int epoch, String state) {
		System.out.println("App"+this.myID+" created record " + name+":"+epoch);
		AppData data = new AppData(name, epoch, state);
		this.appData.put(name, data);
	}

	@Override
	public boolean deleteFinalState(String name, int epoch) {
		AppData data = this.appData.remove(name);
		return (data!=null && data.epoch==epoch ? true : false);
	}

	@Override
	public Integer getEpoch(String name) {
		AppData data = this.appData.get(name);
		if(data!=null) {return data.epoch;}
		return null;
	}
}
