package edu.umass.cs.nio.nioutils;

import org.json.JSONObject;

import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;

/**
 * @author V. Arun
 * 
 *         Default packet multiplexer that is a no-op. This class is used 
 *         either for testing or as a trivial multiplexer starting point
 *         upon which more packet multiplexing functions could be built.
 */
public class PacketDemultiplexerDefault extends AbstractJSONPacketDemultiplexer {
	@Override
	public final boolean handleMessage(JSONObject jsonObject) {
		NIOInstrumenter.incrPktsRcvd();
		return false; // must remain false;
	}
}
