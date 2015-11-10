/**
 * All the GNS Client Commands which can be invoked by a client are defined using this package. 
 *<p>
 * Provides a means for defining commands that inherit behavior from other commands,
 * a listing of all the commands and methods for instantiating and looking up 
 * all the command classes. Also provides methods for generating command documentation.
 *<p> 
 * {@link GnsCommand} is the superclass for all commands.
 * {@link CommandDefs} list all the supported commands.
 * {@link CommandModule} instantiates all the commands from CommandDefs which are
 * all subclasses of {@link GnsCommand} as well as command search and documentation.
 *<p>
 * The commands are group into sub packages based on the type of the command:
 * account (record creation, deletion and lookup), acl (access control), admin (server control), 
 * data (lookup, update, select), group (group guid).
 * 
 */

package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands;