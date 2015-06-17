package edu.umass.cs.nio;

/**
 * @author arun
 *
 * @param <NodeIDType>
 * @param <MessageType>
 */
public interface InterfaceSSLMessenger<NodeIDType, MessageType> extends
		InterfaceMessenger<NodeIDType, MessageType> {

	/**
	 * @return The client messenger if different from self.
	 */
	public InterfaceAddressMessenger<MessageType> getClientMessenger();

	/**
	 * @param clientMessenger
	 */
	public void setClientMessenger(
			InterfaceAddressMessenger<?> clientMessenger);

}
