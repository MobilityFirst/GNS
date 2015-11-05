package edu.umass.cs.gigapaxos.interfaces;

/**
 * @author arun
 * 
 *         Mainly useful for debugging to print summaries of application
 *         requests in gigapaxos.
 *
 */
public interface SummarizableRequest extends Request {
	/**
	 * @return A compact summary generally in the format name:epoch:type and any
	 *         additional relevant information.
	 */
	public String getSummary();
}
