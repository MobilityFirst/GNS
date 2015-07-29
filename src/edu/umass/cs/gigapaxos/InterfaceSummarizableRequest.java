package edu.umass.cs.gigapaxos;

/**
 * @author arun
 * 
 *         Mainly useful for debugging to print summaries of application
 *         requests in gigapaxos.
 *
 */
public interface InterfaceSummarizableRequest extends InterfaceRequest {
	/**
	 * @return A compact summary generally in the format name:epoch:type and any
	 *         additional relevant information.
	 */
	public String getSummary();
}
