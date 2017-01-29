package edu.umass.cs.gnscommon.packets;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectResponsePacket;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.main.GNSConfig;


public class PacketUtils {


	public static CommandPacket setResult(CommandPacket command,
			ResponsePacket response) {
		return command.setResult(response);
	}


	public static JSONObject getCommand(CommandPacket command) {
		return command.getCommand();
	}


	public static String getOriginatingGUID(CommandPacket commandPacket) {
		return commandPacket != null ? commandPacket.getServiceName() : null;
	}


	public static InternalRequestHeader getInternalRequestHeader(
			SelectResponsePacket<String> selectResponse) {
		return new InternalRequestHeader() {

			@Override
			public long getOriginatingRequestID() {
				return selectResponse.getRequestID();
			}

			@Override
			public String getOriginatingGUID() {
				return selectResponse.getServiceName();
			}

			@Override
			public int getTTL() {
				return 1;
			}

			@Override
			public boolean hasBeenCoordinatedOnce() {
				return false;
			}
		};
	}

	public static InternalRequestHeader getInternalRequestHeader(
			CommandPacket commandPacket) {
		return commandPacket instanceof InternalRequestHeader ? (InternalRequestHeader) commandPacket

				:

				new InternalRequestHeader() {

					String mostRecentQueried = null;

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
						try {
							return commandPacket
									.getCommand().has(
											GNSProtocol.ORIGINATING_GUID
													.toString()) ? commandPacket
									.getCommand().getString(
											GNSProtocol.ORIGINATING_GUID
													.toString()) : (this.mostRecentQueried =PacketUtils
									.getOriginatingGUID(commandPacket));
						} catch (JSONException e) {
							return PacketUtils
									.getOriginatingGUID(commandPacket);
						}
					}

					@Override
					public String getQueryingGUID() {
						return this.mostRecentQueried=PacketUtils
								.getOriginatingGUID(commandPacket);
					}

					@Override
					public int getTTL() {
						try {
							return commandPacket.getCommand().has(
									GNSProtocol.REQUEST_TTL.toString()) ? commandPacket
									.getCommand().getInt(
											GNSProtocol.REQUEST_TTL.toString())
									: InternalRequestHeader.DEFAULT_TTL;
						} catch (JSONException e) {
							return InternalRequestHeader.DEFAULT_TTL;
						}
					}

					@Override
					public boolean hasBeenCoordinatedOnce() {
						try {
							return commandPacket.getCommand().has(
									GNSProtocol.COORD1.toString()) ? commandPacket
									.getCommand().getBoolean(
											GNSProtocol.COORD1.toString())
									: commandPacket.needsCoordination();
						} catch (JSONException e) {
							return commandPacket.needsCoordination();
						}
					}

					public boolean verifyInternal() {
						String proof=null;
						try {
							proof = commandPacket.getCommand().has(
									GNSProtocol.INTERNAL_PROOF.toString()) ? commandPacket
									.getCommand().getString(
											GNSProtocol.INTERNAL_PROOF.toString())
									: null;
						} catch (JSONException e) {
							e.printStackTrace();
						}
						return GNSConfig.getInternalOpSecret().equals(proof);
					}
				};
	}


	public static int getLengthEstimate(Object command) {
		int length = 8;
		try {
			if (command instanceof JSONObject) {
				JSONObject o = ((JSONObject) command);
				for (String key : JSONObject.getNames(o))
					length += key.length() + getLengthEstimate(o.get(key));
			} else if (command instanceof JSONArray) {
				JSONArray a = ((JSONArray) command);
				for (int i = 0; i < a.length(); i++)
					length += getLengthEstimate(a.get(i)) + 8;
			} else if (command instanceof String)
				length += command.toString().length();
			else
				length += 8;
		} catch (JSONException je) {
			je.printStackTrace();
		}

		return length;
	}
}
