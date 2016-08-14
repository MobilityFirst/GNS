package edu.umass.cs.gnscommon.packets;

import org.json.JSONObject;

import edu.umass.cs.gnsserver.gnsapp.packet.Packet;

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

}
