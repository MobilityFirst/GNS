package edu.umass.cs.gnscommon.packets;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;

/**
 * @author arun A utility class for methods relating to {@link Packet}.
 */
public class PacketUtils {

	/**
	 * @param command
	 * @param response
	 * @return CommandPacket {@code command} with {@code response} in it.
	 */
	public static CommandPacket setResult(CommandPacket command,
			ResponsePacket response) {
		return command.setResult(response);
	}

	/**
	 * @param command
	 * @return JSONObject command within CommandPacket {@code command}.
	 */
	public static JSONObject getCommand(CommandPacket command) {
		return command.getCommand();
	}

	/**
	 * @param commandPacket
	 * @return The originatingGUID for {@code CommandPacket}.
	 */
	public static String getOriginatingGUID(CommandPacket commandPacket) {
			return commandPacket!=null ? commandPacket.getServiceName() : null;
	}

	/**
	 * @param commandPacket
	 * @return {@link InternalRequestHeader} if {@code commandPacket} is an
	 *         internal request.
	 */
	public static InternalRequestHeader getInternalRequestHeader(
			CommandPacket commandPacket) {
		return commandPacket instanceof InternalRequestHeader ? (InternalRequestHeader) commandPacket
				/* originatingGUID must be non-null for internal commands to
				 * make sense because it is important for internal commands to
				 * be chargeable to someone; that someone is the originating
				 * GUID. This is especially important for active requests where
				 * the request chain is computed dynamically and is not known a
				 * priori. */
				:  
						
						new InternalRequestHeader() {

					@Override
					public long getOriginatingRequestID() {
						try {
							return commandPacket.getCommand().has(
									GNSProtocol.ORIGINATING_QID.toString()) ? commandPacket
									.getCommand().getLong(
											GNSProtocol.ORIGINATING_QID
													.toString())
									: commandPacket.getRequestID();
						} catch (JSONException e) {
							return commandPacket.getRequestID();
						}
					}

					@Override
					public String getOriginatingGUID() {
						return PacketUtils.getOriginatingGUID(commandPacket);
					}

					@Override
					public int getTTL() {
						try {
							return commandPacket.getCommand().has(GNSProtocol.REQUEST_TTL.toString())
									? 
											commandPacket.getCommand().getInt(
									GNSProtocol.REQUEST_TTL.toString())
									: InternalRequestHeader.DEFAULT_TTL;
						} catch (JSONException e) {
							return InternalRequestHeader.DEFAULT_TTL;
						}
					}

					@Override
					public boolean hasBeenCoordinatedOnce() {
						try {
							return commandPacket.getCommand().has(GNSProtocol.COORD1.toString())
									? commandPacket.getCommand().getBoolean(GNSProtocol.COORD1.toString())
											: commandPacket.needsCoordination();
						} catch (JSONException e) {
							return commandPacket.needsCoordination();
						}
					}
				}
					;
	}
}
