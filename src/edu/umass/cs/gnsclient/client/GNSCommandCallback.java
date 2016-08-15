package edu.umass.cs.gnsclient.client;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.packets.ResponsePacket;

/**
 * @author arun
 *
 */
public abstract class GNSCommandCallback implements RequestCallback {

	/**
	 * This method implements a callback that will be invoked after
	 * {@code command} has been executed. Methods such as getResult() may be
	 * used inside this callback method to process the result.
	 * 
	 * @param command
	 */
	public abstract void callback(CommandPacket command);

	private final CommandPacket command;

	/**
	 * 
	 * This constructor is only meant to be invoked by internal classes and
	 * exists so that we can translate a {@link ResponsePacket} based
	 * callback to a {@link CommandPacket} based callback as the latter is more
	 * intuitive to applications and obviates dealing with the former.
	 * 
	 * @param command
	 * @param callback
	 */
	GNSCommandCallback(CommandPacket command, RequestCallback parentCallback) {
		this.command = command;
	}

	/**
	 * @param command
	 */
	public GNSCommandCallback(CommandPacket command) {
		this(command, null);
	}

	public void handleResponse(Request response) {
		this.callback(command);
	}
}
