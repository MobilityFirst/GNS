package edu.umass.cs.gns.replicaCoordination.multipaxos;

import java.io.IOException;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;


public class PaxosLogger {
	public static enum logcmds {LOG_AND_SEND_MSG};
	
	private static Logger log = Logger.getLogger(PaxosLogger.class.getName()); // GNS.getLogger();

	public void logMessage(String paxosID, JSONObject msg) {
		
	}
	public void logMessageAndSendReply(String paxosID, JSONObject msg, MessagingTask reply) {
		
	}
	public void logMessageAndExecute(String paxosID, JSONObject msg) {
		
	}
	public void logAndMessage(LogMessagingTask logMTask, PaxosMessenger messenger) throws JSONException, IOException {
		log.info("Logging: " + logMTask.logMsg.toString());
		messenger.send(logMTask);
	}
}