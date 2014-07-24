package scredis.commands

import scredis.io.NonBlockingConnection
import scredis.protocol.requests.ServerRequests._
import scredis.serialization.{ Reader, Writer }

import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * This trait implements server commands.
 *
 * @define e [[scredis.exceptions.RedisErrorResponseException]]
 * @define none `None`
 * @define true '''true'''
 * @define false '''false'''
 */
trait ServerCommands { self: NonBlockingConnection =>
  
  /**
   * Asynchronously rewrites the append-only file.
   *
   * @since 1.0.0
   */
  def bgRewriteAOF(): Future[Unit] = send(BGRewriteAOF())
  
  /**
   * Asynchronously saves the dataset to disk.
   *
   * @throws $e if the background save is already in progress
   *
   * @since 1.0.0
   */
  def bgSave(): Future[Unit] = send(BGSave())
  
  /**
   * Get the current client name.
   *
   * @return option containing the name if it has been set, $none otherwise
   *
   * @since 2.6.9
   */
  def clientGetName(): Future[Option[String]] = send(ClientGetName())
  
  /**
   * Kills the connection of a client.
   *
   * @param ip ip address of the client to kill
   * @param port port of the client to kill
   * @throws $e if the the client does not exist
   *
   * @since 2.4.0
   */
  def clientKill(ip: String, port: Int): Future[Unit] = send(ClientKill(ip, port))
  
  /**
   * Kills the connection of potentially multiple clients satisfying various filters.
   *
   * @param addrs address(es) of the client(s) to be kill, in the form of (ip, port) pairs
   * @param ids id(s) of the client(s) to kill
   * @param types type(s) of the client(s) to kill, namely `normal`, `slave` or `pubsub`
   * @param skipMe when $true, the calling client will not be killed, when $false, the latter can
   * be killed
   * @return the number of killed client(s)
   *
   * @since 2.8.12
   */
  def clientKillWithFilters(
    addrOpt: Option[(String, Int)] = None,
    idOpt: Option[Long] = None,
    typeOpt: Option[scredis.ClientType] = None,
    skipMe: Boolean = true
  ): Future[Long] = (addrOpt, idOpt, typeOpt) match {
    case (None, None, None) => Future.successful(0)
    case _ => send(ClientKillWithFilters(addrOpt, idOpt, typeOpt, skipMe))
  }
  
  /**
   * Lists all client connections.
   *
   * @return list of clients
   *
   * @since 2.4.0
   */
  def clientList(): Future[List[Map[String, String]]] = send(ClientList())
  
  /**
   * Suspends all clients for the specified amount of time.
   *
   * @param timeoutMillis the amount of time in milliseconds for which clients should be suspended
   * @throws $e if the timeout is invalid
   *
   * @since 2.9.50
   */
  def clientPause(timeoutMillis: Long): Future[Unit] = send(ClientPause(timeoutMillis))
  
  /**
   * Sets the current client name. If the empty string is provided, the name will be unset.
   *
   * @param name name to associate the client to, if empty, unsets the client name
   *
   * @since 2.6.9
   */
  def clientSetName(name: String): Future[Unit] = send(ClientSetName(name))
  
  /**
   * Returns details about all `Redis` commands.
   *
   * @return map of command name to command info
   *
   * @since 2.8.13
   */
  def command(): Future[Map[String, scredis.CommandInfo]] = send(Command())
  
  /**
   * Returns the total number of commands in the target `Redis` server.
   *
   * @return total number of commands
   *
   * @since 2.8.13
   */
  def commandCount(): Future[Int] = send(CommandCount())
  
  /**
   * Returns the list of keys part of a full `Redis` command.
   *
   * @return the list of keys present in the command
   *
   * @since 2.8.13
   */
  def commandGetKeys(command: String): Future[List[String]] = send(CommandGetKeys[List](command))
  
  /**
   * Returns details about the specified `Redis` commands.
   *
   * @param names command names
   * @return map of command name to command info
   *
   * @since 2.8.13
   */
  def commandInfo(names: String*): Future[Map[String, scredis.CommandInfo]] = {
    if (names.isEmpty) {
      Future.successful(Map.empty)
    } else {
      send(CommandInfo(names: _*))
    }
  }
  
  /**
   * Gets the value of a configuration parameter.
   *
   * @param pattern name or pattern of the configuration parameter to get
   * @return option containing the matched parameters, or $none if no parameters are matched
   *
   * @since 2.0.0
   */
  def configGet(pattern: String = "*"): Future[Map[String, String]] = send(ConfigGet(pattern))
  
  /**
   * Resets the stats returned by INFO.
   *
   * @since 2.0.0
   */
  def configResetStat(): Future[Unit] = send(ConfigResetStat())
  
  /**
   * Rewrites the redis.conf file the server was started with, applying the minimal changes
   * needed to make it reflect the configuration currently used by the server, which may be
   * different compared to the original one because of the use of the CONFIG SET command.
   *
   * @throws $e if the configuration file was not properly written
   * 
   * @since 2.8.0
   */
  def configRewrite(): Future[Unit] = send(ConfigRewrite())
  
  /**
   * Sets a configuration parameter to the given value.
   *
   * @param parameter parameter's name
   * @param value value to set parameter to
   * @throws $e if the parameter could not be set
   *
   * @since 2.0.0
   */
  def configSet[W: Writer](parameter: String, value: W): Future[Unit] = send(
    ConfigSet(parameter, value)
  )
  
  /**
   * Return the number of keys in the selected database.
   *
   * @return number of keys in the selected database
   *
   * @since 1.0.0
   */
  def dbSize(): Future[Long] = send(DBSize())
  
  /**
   * Removes all keys from all databases.
   *
   * @since 1.0.0
   */
  def flushAll(): Future[Unit] = send(FlushAll())
  
  /**
   * Removes all keys from the current database.
   *
   * @since 1.0.0
   */
  def flushDB(): Future[Unit] = send(FlushDB())
  
  /**
   * Gets information and statistics about the server.
   *
   * @note The section can additionally take the following values: `all` and `default`.
   * 
   * @param section name of the section for which data should be retrieved
   * @return map of field -> value pairs that match the specified section, or an empty map
   * if the section does not exist
   *
   * @since 1.0.0
   */
  def info(section: String = "default"): Future[Map[String, String]] = {
    val sectionOpt = if (section != "default") {
      Some(section)
    } else {
      None
    }
    send(Info(sectionOpt))
  }
  
  /**
   * Gets the UNIX timestamp of the last successful save to disk.
   *
   * @return UNIX timestamp
   *
   * @since 1.0.0
   */
  def lastSave(): Future[Long] = send(LastSave())
  
  /**
   * Provides information on the role of a Redis instance in the context of replication,
   * by returing if the instance is currently a master, slave, or sentinel.
   * 
   * @note The command also returns additional information about the state of the replication
   * (if the role is master or slave) or the list of monitored master names (if the role is
   * sentinel).
   *
   * @return the [[scredis.Role]] of the `Redis` instance
   *
   * @since 2.8.12
   */
  def role(): Future[scredis.Role] = send(Role())
  
  /**
   * Synchronously saves the dataset to disk.
   *
   * @since 1.0.0
   */
  def save(): Future[Unit] = send(Save())
  
  /**
   * Gracefully shuts down a `Redis` server.
   *
   * @note The command performs the following operations:
   * - Stops all the clients
   * - Performs a blocking `SAVE` if at least one '''save point''' is configured
   * - Flushes the Append Only File if AOF is enabled
   * - Quits the server
   * 
   * @param modifierOpt optional [[scredis.ShutdownModifier]]
   * 
   * @since 1.0.0
   */
  def shutdown(modifierOpt: Option[scredis.ShutdownModifier] = None): Future[Unit] = send(
    Shutdown(modifierOpt)
  )
  
  /**
   * Configures the current `Redis` instance as a slave of another `Redis` instance.
   * 
   * @param host the host of the master instance
   * @param port the port of the master instance
   *
   * @since 1.0.0
   */
  def slaveOf(host: String, port: Int): Future[Unit] = send(SlaveOf(host, port))
  
  /**
   * Breaks the replication of a slave `instance`, turning it back into a master instance.
   *
   * @since 1.0.0
   */
  def slaveOfNoOne(): Future[Unit] = send(SlaveOfNoOne())
  
  /**
   * Retieves entries from the slowlog.
   * 
   * @param countOpt optionally limits the number of retrieved entries
   * @return list of slowlog entries
   * 
   * @since 2.2.12
   */
  def slowLogGet(countOpt: Option[Int] = None): Future[List[scredis.SlowLogEntry]] = send(
    SlowLogGet[List](countOpt)
  )
  
  /**
   * Returns the number of entries in the slowlog.
   * 
   * @return number of entries in the slowlog
   * 
   * @since 2.2.12
   */
  def slowLogLen(): Future[Long] = send(SlowLogLen())
  
  /**
   * Resets the slowlog.
   * 
   * @since 2.2.12
   */
  def slowLogReset(): Future[Unit] = send(SlowLogReset())
  
  /**
   * Returns the current server time.
   *
   * @return pair of longs containing (1) UNIX timestamp and (2) microseconds
   *
   * @since 2.6.0
   */
  def time(): Future[(Long, Long)] = send(Time())
  
}