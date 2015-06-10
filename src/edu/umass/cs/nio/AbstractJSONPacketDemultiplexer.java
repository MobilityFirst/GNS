package edu.umass.cs.nio;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.nioutils.NIOInstrumenter;

/**
 * @author V. Arun
 */
public abstract class AbstractJSONPacketDemultiplexer extends
		AbstractPacketDemultiplexer<JSONObject> implements
		InterfacePacketDemultiplexer<JSONObject> {

	/**
	 * 
	 * @param threadPoolSize
	 *            Refer documentation for {@link AbstractPacketDemultiplexer#setThreadPoolSize(int)
	 *            setThreadPoolsize(int)}.
	 */
	protected AbstractJSONPacketDemultiplexer(int threadPoolSize) {
		super(threadPoolSize);
	}

	protected AbstractJSONPacketDemultiplexer() {
		super();
	}
	
	protected Integer getPacketType(JSONObject json) {
		try {
			return JSONPacket.getPacketType(json);
		} catch (JSONException e) {
			log.severe("Unable to decode JSON packet type for: " + json);
			e.printStackTrace();
		}
		return null;
	}
	
	protected JSONObject getMessage(String msg) {
		return MessageExtractor.parseJSON((String) msg);
	}

	// This method will be invoked by NIO
	protected boolean handleMessageSuper(JSONObject jsonObject)
			throws JSONException {
		NIOInstrumenter.rcvdJSONPacket(jsonObject);
		return super.handleMessageSuper(jsonObject);
	}

}
