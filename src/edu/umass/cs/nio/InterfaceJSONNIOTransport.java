package edu.umass.cs.nio;

import org.json.JSONObject;

/**
 * @author V. Arun
 * 
 *         This interface was useful primarily for backwards compatibility and
 *         is deprecated now. Use just {@link InterfaceMessenger
 *         InterfaceMessenger} or, if really needed,
 *         {@link InterfaceNIOTransport
 *         InterfaceNIOTransport<NodeIDType,JSONObject}instead.
 * 
 * @param <NodeIDType>
 */
@Deprecated
public interface InterfaceJSONNIOTransport<NodeIDType> extends
		InterfaceNIOTransport<NodeIDType, JSONObject> {
}
