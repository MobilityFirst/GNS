package edu.umass.cs.gns.reconfiguration;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.nio.JSONMessenger;

/**
 * @author V. Arun
 */
/* This abstract class should be inherited by replica coordination 
 * protocols. The only abstract method the inheritor needs to 
 * implement is coordinateRequest(.). For example, this method 
 * could lazily propagate the request to all replicas; or it could
 * ensure reliable receipt from a threshold number of replicas for
 * durability, and so on.
 * 
 * Notes:
 * 
 * In general, coordinateRequest(.) is app-specific logic. But most 
 * common cases, e.g., linearizability, sequential, causal, eventual,
 * etc. can be implemented agnostic to app-specific details with
 * supporting libraries that we can provide.
 * 
 * A request is not an entity that definitively does or does not need 
 * coordination. A request, e.g., a read request, may or may not need 
 * coordination depending on the replica coordination protocol, which 
 * is why it is here.
 * 
 */
public abstract class AbstractReplicaCoordinator<NodeIDType> implements
		InterfaceRepliconfigurable, InterfaceReplicaCoordinator<NodeIDType> {
	protected final InterfaceRepliconfigurable app;
	private final ConcurrentHashMap<IntegerPacketType, Boolean> coordinationTypes =
			new ConcurrentHashMap<IntegerPacketType, Boolean>();
	
	private InterfaceReconfiguratorCallback callback = null;
	private InterfaceReconfiguratorCallback activeCallback = null; // for stops
	private boolean largeCheckpoints = false;
	protected JSONMessenger<NodeIDType> messenger;	

	/********************* Start of abstract methods **********************************************/
	/* This method performs whatever replica coordination action is necessary to handle the request.
	 */
	public abstract boolean coordinateRequest(
			InterfaceRequest request) throws IOException, RequestParseException;

	/* This method should return true if the replica group is successfully created or 
	 * one already exists with the same set of nodes. It should return false otherwise.
	 */
	public abstract boolean createReplicaGroup(String serviceName, int epoch, String state, Set<NodeIDType> nodes);

	/* This method should result in all state corresponding to serviceName being deleted */
	public abstract void deleteReplicaGroup(String serviceName);

	/* This method must return the replica group that was most recently successfully created
	 * for the serviceName using createReplicaGroup.
	 */
	public abstract Set<NodeIDType> getReplicaGroup(String serviceName);
	/********************* End of abstract methods ***********************************************/

	// A replica coordinator is meaningless without an underlying app
	protected AbstractReplicaCoordinator(InterfaceReplicable app) {
		this.app = new TrivialRepliconfigurable(app); 
		this.messenger = null;
	}
	public AbstractReplicaCoordinator(InterfaceReplicable app, JSONMessenger<NodeIDType> messenger) {
		this(app);
		this.messenger = messenger;
	}
	
	protected void setMessenger(JSONMessenger<NodeIDType> messenger) {
		this.messenger = messenger;
	}

	// Registers request types that need coordination
	protected void registerCoordination(IntegerPacketType type) {
		this.coordinationTypes.put(type, true);
	}

	protected void registerCoordination(IntegerPacketType[] types) {
		for (IntegerPacketType type : types)
			this.registerCoordination(type);
	}

	protected final AbstractReplicaCoordinator<NodeIDType> setCallback(InterfaceReconfiguratorCallback callback) {
		this.callback = callback;
		return this;
	}
	protected final AbstractReplicaCoordinator<NodeIDType> setActiveCallback(InterfaceReconfiguratorCallback callback) {
		this.activeCallback = callback;
		return this;
	}

	// Coordinate if needed, else hand over to app
	public boolean handleIncoming(InterfaceRequest request) {
		boolean handled = false;
		if (needsCoordination(request)) {
			try {
				((InterfaceReplicableRequest)request).setNeedsCoordination(false);
				coordinateRequest(request);
				handled = false;
			} catch(IOException ioe) {
				ioe.printStackTrace();
			} catch(RequestParseException rpe) {
				rpe.printStackTrace();
			} 
		}
		else {
			handled = this.handleRequest(request);
		}
		return handled;
	}

	public boolean handleRequest(InterfaceRequest request) {
		return this.handleRequest(request, false);
	}
	/* This method is a wrapper for Application.handleRequest and meant to be invoked
	 * by the class that implements this AbstractReplicaCoordinator or its helper classes.
	 * 
	 * We need control over this method in order to call the callback after the app's
	 * handleRequest method has been executed. An alternative would have been to 
	 * enforce the callback as part of the Reconfigurable interface. However, this 
	 * is a less preferred design because it depends more on the app's support for
	 * stop requests even though a stop request is really meaningless to an app.
	 */
	public boolean handleRequest(InterfaceRequest request, boolean noReplyToClient) {

		boolean handled = ((this.app instanceof InterfaceReplicable) ? 
				((InterfaceReplicable)(this.app)).handleRequest(request, noReplyToClient) : 
					this.app.handleRequest(request));
		callCallback(request, handled);
		/* We always return true because the return value here is a no-op. It might
		 * as well be void. Returning anything but true will ensure that a paxos
		 * coordinator will get stuck on this request forever. The app can still
		 * convey false if needed to the caller via the callback.
		 */
		return true;
	}
	public InterfaceRequest getRequest(String stringified) throws RequestParseException {
		return this.app.getRequest(stringified);
	}
	/* Need to return just app's request types. Coordination packets can 
	 * go directly to the coordination module and don't have to be known
	 * to ActiveReplica.
	 */
	public Set<IntegerPacketType> getAppRequestTypes() {
		return this.app.getRequestTypes();
	}

	@Override
	public InterfaceReconfigurableRequest getStopRequest(String name, int epoch) {
		return this.app.getStopRequest(name, epoch);
	}
	
	@Override
	public String getFinalState(String name, int epoch) {
		return this.app.getFinalState(name, epoch);
	}

	@Override
	public void putInitialState(String name, int epoch, String state) {
		this.app.putInitialState(name, epoch, state);
	}

	@Override
	public boolean deleteFinalState(String name, int epoch) {
		return this.app.deleteFinalState(name, epoch);
	}
	
	@Override
	public Integer getEpoch(String name) {
		return this.app.getEpoch(name);
	}
	
	@Override
	public String getState(String name) {
		return app.getState(name);
	}

	@Override
	public boolean updateState(String name, String state) {
		return app.updateState(name, state);
	}

	
	/*********************** Start of private helper methods **********************/
	// Call back active replica for stop requests, else call default callback
	private void callCallback(InterfaceRequest request, boolean handled) {
		if (request instanceof InterfaceReconfigurableRequest
				&& ((InterfaceReconfigurableRequest) request).isStop()
				&& this.activeCallback != null)
			this.activeCallback.executed(request, handled);
		else if (this.callback != null)
			this.callback.executed(request, handled);
	}

	private boolean needsCoordination(InterfaceRequest request) {
		if (request instanceof InterfaceReplicableRequest
				&& ((InterfaceReplicableRequest) request).needsCoordination()) {
			return true; // this.coordinationTypes.containsKey(request.getRequestType());
		}
		return false;
	}
	public NodeIDType getMyID() {
		assert(this.messenger!=null);
		return this.messenger.getMyID();
	}
	
	public boolean hasLargeCheckpoints() {
		return this.largeCheckpoints;
	}
	public void setLargeCheckpoints() {
		this.largeCheckpoints = true;
	}
	/*********************** End of private helper methods ************************/

	/********************** Request propagation helper methods ******************/
	/*
	 * FIXME: This sendAllLazy method is concretized for JSON and is present
	 * here only for convenience, so that inheritors can directly use it.
	 * JSONObject is not needed anywhere else in this class and should ideally
	 * not be in this class at all.
	 */
	protected void sendAllLazy(InterfaceRequest request) throws IOException, RequestParseException, JSONException {
		assert(request.getServiceName()!=null);
		assert(this.getReplicaGroup(request.getServiceName())!=null) : "ARC"+getMyID()+" has no group for " + request.getServiceName();
		GenericMessagingTask<NodeIDType,Object> mtask = 
				new GenericMessagingTask<NodeIDType,Object>(this.getReplicaGroup(request.getServiceName()).toArray(), 
						new JSONObject(request.toString())); 
		if(this.messenger==null) return; 
		this.messenger.send(mtask);
	}
}
