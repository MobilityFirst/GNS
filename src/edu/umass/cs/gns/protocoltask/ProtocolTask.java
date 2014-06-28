package edu.umass.cs.gns.protocoltask;

import edu.umass.cs.gns.nio.MessagingTask;
import edu.umass.cs.gns.util.Keyable;

/**
 * @author V. Arun
 */

/*
 * A protocol task is broadly a task consisting of a sequence of
 * asynchronous steps, where an asynchronous step is one that needs
 * to wait for some message(s) from remote node(s) in order to finish.
 * 
 * A protocol task is represented as an extended Finite State Machine
 * as this allows us to implement a sequence of event/action steps. We
 * call it an extended FSM because an FSM state, strictly speaking,
 * does not have "memory"; or if it needs to have memory, we need
 * to create a separate state for each value the corresponding
 * register could take. But it is convenient to think of most
 * event/action protocols as FSMs whose states have some memory,
 * e.g., a counter in a "wait_for_replies" state that is incremented
 * with each new reply receipt without the state itself changing
 * until a threshold number of replies are received.
 * 
 * What is this abstraction good for? The following use cases:
 * 
 * (1) Retransmission until delivered.
 * Note: With reliable NIO, you should be extremely careful with
 * retransmissions. We should really just let NIO handle most
 * retransmissions, otherwise there is a risk of recklessly
 * overloading the system. This interface will take care of
 * retransmissions internally. If this interface can not,
 * then no one else can, and re-trying yourself is futile.
 * 
 * (2) Getting a value from a remote node.
 * 
 * (3) Getting replies from a threshold subset of nodes.
 * 
 * (4) A sequence of any of the above steps.
 */

public interface ProtocolTask<EventType extends Comparable<EventType>, KeyType extends Comparable<KeyType>>
		extends Keyable<KeyType> {

	/*
	 * The action handling the event may return messaging tasks that will be automatically
	 * handled by this interface. This interface lets the protocol developer focus on
	 * event/action pairs without worrying about networking or scheduling optimizations.
	 * The parameter ptasks[0] returns a single protocol task if any spawned by this action.
	 */
	public MessagingTask[] handleEvent(ProtocolEvent<EventType, KeyType> event,
			ProtocolTask<EventType, KeyType>[] ptasks);

	public MessagingTask[] start(); // actions to be executed in the beginning
	// To cancel, invoke ProtocolExecutor.cancel(this)

	/*
	 * We need a unique key in order to match incoming messages against the
	 * corresponding task, so this key also needs to be present
	 * in every message exchanged as part of the protocol task. The getKey
	 * method is necessitated by the Keyable<KeyType> interface above. The
	 * refreshKey() method below should return a random key that is unique
	 * across nodes. It needs to be implemented by the instantiator
	 * because we don't know the concrete type of KeyType. Look at
	 * PingPongProtocolTask for an example.
	 */

	public KeyType refreshKey(); // should return a random key that is unique across nodes
}
