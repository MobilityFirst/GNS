package edu.umass.cs.gnsserver.gnsapp.selectpolicy;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;


import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnsserver.gnsapp.GNSApplicationInterface;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectRequestPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectResponsePacket;


/**
 * This class is an abstract class for specifying a select policy.
 * 
 * The object of this class is created based on Java reflection, 
 * by taking the class name as input from the GNS config files. 
 * The child class must implement a constructor that takes {@link GNSApplicationInterface}
 * as input. This class also has a cyclic dependency with {@link GNSApp} but that is needed
 * to perform signature checking for select requests. 
 * 
 * @author ayadav
 */
public abstract class AbstractSelectPolicy
{
	protected static final Logger LOG = Logger.getLogger(AbstractSelectPolicy.class.getName());
	
	protected final GNSApplicationInterface<String> gnsApp;
	
	public AbstractSelectPolicy(GNSApplicationInterface<String> gnsApp)
	{
		this.gnsApp = gnsApp;
	}
	
	/**
	 * This method processes a select request from a client in a non-blocking manner.
	 * 
	 * One thing to note is that this method doesn't need to be blocking. 
	 * The code chain for this method is as follows. The gigapaxos calls {@link GNSApp#execute}
	 * method, in the COMMAND case, if the command is a select command then we call this method. 
	 * If the command is not a select command then we process as usual in the 
	 * {@link GNSApp#execute}.
	 * 
	 * The  {@link GNSApp#execute} method doesn't need to block for the completion of a
	 * SelectRequest, i.e., {@link GNSApp#execute} calls 
	 * {@link #handleSelectRequestFromClient(CommandPacket)} method and 
	 * this method forwards the select request to the  required NSs and returns.
	 * 
	 * 
	 * @param request, the incoming request
	 * @return Returns {@link Future}, which the caller of 
	 * {@link #handleSelectRequestFromClient(CommandPacket)} can use 
	 * to wait for the completion. 
	 */
	public abstract Future<Boolean> handleSelectRequestFromClient(CommandPacket request);
	
	
	/**
	 * This method is called in the SELECT_REQUEST case of {@link GNSApp#execute}
	 * This method is called on NSs that process a select request and return GUIDs  
	 * to the select request's originating NS. 
	 * 
	 * This method performs DB operations but it is not blocking. For improving the 
	 * performance we can perform DB operations in a separate thread pool, so that
	 * the gigapaxos or NIO threads, which are small in number, don't block for
	 * DB operations.  
	 * 
	 * 
	 * @param selectReq
	 */
	public abstract void handleSelectRequestAtNS(SelectRequestPacket selectReq);
	
	
	/**
	 * This method is called at a select request originating NS when another NS
	 * replies back for an earlier select request. 
	 * In this method, we aggregate the response GUIDs and if all the contacted NSs have 
	 * responded then we send a reply back to the client. 
	 * 
	 * 
	 * @param selectResp
	 */
	public abstract void handleSelectResponseFromNS(SelectResponsePacket selectResp); 
	
	
	/**
	 * Creates the object of clazz by reflection. 
	 * clazz will be a child class of {@link AbstractSelectPolicy} whose 
	 * class path will be specified in the GNS config files.
	 * 
	 * @param clazz
	 * @param gnsApp
	 * @return null if the object creation fails.
	 */
	public static AbstractSelectPolicy createSelectObject(Class<?> clazz, 
											GNSApplicationInterface<?> gnsApp) 
	{
		try
		{
			return (AbstractSelectPolicy) clazz.getConstructor
					(GNSApplicationInterface.class).newInstance(gnsApp);
		} 
		catch (InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) 
		{
			LOG.log(Level.SEVERE, 
					e.getClass().getSimpleName() + " while creating " + clazz);
			e.printStackTrace();
		}
		return null;
	}
}