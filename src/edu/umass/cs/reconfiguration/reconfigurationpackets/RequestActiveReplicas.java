package edu.umass.cs.reconfiguration.reconfigurationpackets;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.nio.Stringifiable;
import edu.umass.cs.nio.StringifiableDefault;

/**
 * @author V. Arun
 */

/*
 * This class is used to both request for and return the set of active replicas
 * for a name. If the field actives is null, it is implicitly interpreted as a
 * request, else as a response.
 */
public class RequestActiveReplicas extends
		BasicReconfigurationPacket<InetSocketAddress> {

	private static enum Keys {
		ACTIVE_REPLICAS, FAILED
	};

	/**
	 * Unstringer for InetSocketAddress of sender.
	 */
	public static final Stringifiable<InetSocketAddress> unstringer = new StringifiableDefault<InetSocketAddress>(
			new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));

	private Set<InetSocketAddress> actives = null;
	private boolean failed = false;

	/**
	 * @param initiator
	 * @param name
	 * @param epochNumber
	 */
	public RequestActiveReplicas(InetSocketAddress initiator, String name,
			int epochNumber) {
		super(initiator,
				ReconfigurationPacket.PacketType.REQUEST_ACTIVE_REPLICAS, name,
				epochNumber);
		this.actives = null;
	}

	/**
	 * @param json
	 * @param unstringer
	 * @throws JSONException
	 */
	public RequestActiveReplicas(JSONObject json, Stringifiable<?> unstringer)
			throws JSONException {
		super(json, RequestActiveReplicas.unstringer); // ignores arg unstringer
		this.setSender(JSONNIOTransport.getSenderAddress(json));
		this.failed = json.optBoolean(DeleteServiceName.Keys.FAILED.toString());

		JSONArray jsonArray = json.has(Keys.ACTIVE_REPLICAS.toString()) ? json
				.getJSONArray(Keys.ACTIVE_REPLICAS.toString()) : null;
		if (jsonArray != null) {
			this.actives = new HashSet<InetSocketAddress>();
			for (int i = 0; jsonArray != null && i < jsonArray.length(); i++)
				this.actives.add(RequestActiveReplicas.unstringer
						.valueOf(jsonArray.get(i).toString()));
		}
	}

	/**
	 * @param json
	 * @throws JSONException
	 */
	public RequestActiveReplicas(JSONObject json) throws JSONException {
		this(json, null);
	}

	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		if (this.actives != null)
			json.put(Keys.ACTIVE_REPLICAS.toString(), new JSONArray(
					this.actives));
		json.put(DeleteServiceName.Keys.FAILED.toString(), failed);
		return json;
	}

	/**
	 * @param replicas
	 */
	public void setActives(Set<InetSocketAddress> replicas) {
		this.actives = replicas;
	}

	/**
	 * @return Active replica socket addresses.
	 */
	public Set<InetSocketAddress> getActives() {
		return this.actives;
	}

	/**
	 * @return Returns this after setting as failed.
	 */
	public RequestActiveReplicas setFailed() {
		this.failed = true;
		return this;
	}

	/**
	 * @return Whether this request failed.
	 */
	public boolean isFailed() {
		return this.failed;
	}

	static void main(String[] args) {
		String[] addrs = { "128.119.240.21" };
		int[] ports = { 3245 };
		assert (addrs.length == ports.length);
		InetSocketAddress[] isaddrs = new InetSocketAddress[addrs.length];
		try {
			for (int i = 0; i < addrs.length; i++) {
				isaddrs[i] = new InetSocketAddress(
						InetAddress.getByName(addrs[i]), ports[i]);
			}
			String name = "name";
			InetSocketAddress sender = new InetSocketAddress(
					InetAddress.getLoopbackAddress(), 1234);
			RequestActiveReplicas req1 = new RequestActiveReplicas(sender,
					name, 0);
			System.out.println(req1);
			JSONObject json1;
			json1 = req1.toJSONObject();
			json1.put(JSONNIOTransport.DEFAULT_IP_FIELD, sender.getAddress()
					.getHostAddress());
			json1.put(JSONNIOTransport.DEFAULT_PORT_FIELD, sender.getPort());
			RequestActiveReplicas req2 = new RequestActiveReplicas(json1, null);
			System.out.println(req2);
		} catch (UnknownHostException | JSONException e) {
			e.printStackTrace();
		}
	}
}
