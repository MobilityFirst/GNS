package edu.umass.cs.protocoltask;

/**
 * @author arun
 * 
 *         Thrown if a protocol task creation encounters errors, typically
 *         because a task with the same key is already running.
 *
 */
public class ProtocolTaskCreationException extends RuntimeException {
	private static final long serialVersionUID = ProtocolTaskCreationException.class.hashCode();
	ProtocolTaskCreationException(String msg) {
		super(msg);
	}

}
