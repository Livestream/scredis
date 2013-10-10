/*
 * Copyright (c) 2013 Livestream LLC. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package scredis.commands

import scredis.CommandOptions
import scredis.protocol.Protocol
import scredis.parsing.Implicits._

/**
 * This trait implements server commands.
 *
 * @define none `None`
 * @define e [[scredis.exceptions.RedisCommandException]]
 */
trait ServerCommands { self: Protocol =>
  import Names._

  /**
   * Asynchronously rewrites the append-only file.
   *
   * @since 1.0.0
   */
  def bgRewriteAOF()(implicit opts: CommandOptions = DefaultCommandOptions): Unit =
    send(BgRewriteAOF)(asUnit)

  /**
   * Asynchronously saves the dataset to disk.
   *
   * @throws $e if the background save is already in progress
   *
   * @since 1.0.0
   */
  def bgSave()(implicit opts: CommandOptions = DefaultCommandOptions): Unit = send(BgSave)(asUnit)

  /**
   * Get the current client name.
   *
   * @return option containing the name if it has been set, $none otherwise
   *
   * @since 2.6.9
   */
  def clientGetName()(implicit opts: CommandOptions = DefaultCommandOptions): Option[String] =
    send(Client, ClientGetName)(asBulk[String])

  /**
   * Kills the connection of a client.
   *
   * @param addr string containing ip and port separated by colon, i.e. "ip:port"
   * @throws  RedisCommandException if the the client does not exist
   *
   * @since 2.4.0
   */
  def clientKill(addr: String)(implicit opts: CommandOptions = DefaultCommandOptions): Unit =
    send(Client, ClientKill, addr)(asUnit)

  /**
   * Kills the connection of a client.
   *
   * @param ip ip address of the target client
   * @param port port of the target client
   * @throws  RedisCommandException if the the client does not exist
   *
   * @since 2.4.0
   */
  def clientKillFromIpPort(ip: String, port: Int)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Unit = clientKill("%s:%d".format(ip, port))

  /**
   * Gets the list of client connections.
   *
   * @return raw string containing the list of clients as returned by Redis
   *
   * @since 2.4.0
   */
  def clientListRaw()(implicit opts: CommandOptions = DefaultCommandOptions): String =
    send(Client, ClientList)(asBulk[String, String](flatten))

  /**
   * Gets the list of client connections.
   *
   * @return list of clients
   *
   * @since 2.4.0
   */
  def clientList()(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): List[Map[String, String]] = send(Client, ClientList)(
    asBulk[String, List[Map[String, String]]](string => {
      string.get.split("\n").map(_.split(" ").flatMap(p => {
        val split = p.split("=")
        if (split.size == 2) Some((split(0) -> split(1)))
        else None
      }).toMap).toList
    })
  )

  /**
   * Sets the current client name. If the empty string is provided, the name will be unset.
   *
   * @param name name to associate the client to, if empty, unsets the client name
   *
   * @since 2.6.9
   */
  def clientSetName(name: String)(implicit opts: CommandOptions = DefaultCommandOptions): Unit =
    send(Client, ClientSetName, name)(asUnit)

  /**
   * Gets the value of a configuration parameter.
   *
   * @param pattern name or pattern of the configuration parameter to get
   * @return option containing the matched parameters, or $none if no parameters are matched
   *
   * @since 2.0.0
   */
  def configGet(pattern: String = "*")(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Option[Map[String, Option[String]]] = send(Config, ConfigGet, pattern)(
    asMultiBulk[List, Option[Map[String, Option[String]]]](x =>
      toOptionalMap[String, String](x).map(_.collect {
        case (key, value) => if (value.isEmpty) (key, None) else (key, Some(value))
      })
    )
  )

  /**
   * Resets the stats returned by INFO.
   *
   * @since 2.0.0
   */
  def configResetStat()(implicit opts: CommandOptions = DefaultCommandOptions): Unit =
    send(Config, ConfigResetStat)(asUnit)

  /**
   * Sets a configuration parameter to the given value.
   *
   * @param key parameter's name
   * @param value value to set parameter to
   * @throws $e if the parameter could not be set
   *
   * @since 2.0.0
   */
  def configSet(key: String, value: Any)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Unit = send(Config, ConfigSet, key, value)(asUnit)

  /**
   * Return the number of keys in the selected database.
   *
   * @return number of keys in the selected database
   *
   * @since 1.0.0
   */
  def dbSize()(implicit opts: CommandOptions = DefaultCommandOptions): Long =
    send(DbSize)(asInteger)

  /**
   * Removes all keys from all databases.
   *
   * @since 1.0.0
   */
  def flushAll()(implicit opts: CommandOptions = DefaultCommandOptions): Unit =
    send(FlushAll)(asUnit)

  /**
   * Removes all keys from the current database.
   *
   * @since 1.0.0
   */
  def flushDb()(implicit opts: CommandOptions = DefaultCommandOptions): Unit = send(FlushDb)(asUnit)

  /**
   * Gets information and statistics about the server.
   *
   * @return raw string containing lines of field:value pairs as returned by Redis
   *
   * @since 1.0.0
   */
  def infoRaw()(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): String = send(Info)(asBulk[String, String](flatten))
  
  /**
   * Gets information and statistics about the server.
   *
   * @param section name of the section for which data should be retrieved
   * @return raw string containing lines of field:value pairs that match the specified section,
   * as returned by Redis, or an empty string if the section does not exist
   *
   * @since 2.6.0
   */
  def infoBySectionRaw(section: String = "default")(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): String = send(Info, section)(asBulk[String, String](flatten))

  /**
   * Gets information and statistics about the server.
   *
   * @return map of field -> value pairs
   *
   * @since 1.0.0
   */
  def info()(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Map[String, String] = send(Info)(asBulk[String, Map[String, String]](option => {
    option.get.split("\r\n").withFilter(s => {
      s.isEmpty == false && s.startsWith("#") == false
    }).map(p => {
      val split = p.split(":")
      (split(0) -> split(1))
    }).toMap
  }))
  
  /**
   * Gets information and statistics about the server.
   *
   * @param section name of the section for which data should be retrieved
   * @return map of field -> value pairs that match the specified section, or an empty map
   * if the section does not exist
   *
   * @since 2.6.0
   */
  def infoBySection(section: String = "default")(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Map[String, String] = send(Info, section)(asBulk[String, Map[String, String]](option => {
    option.get.split("\r\n").withFilter(s => {
      s.isEmpty == false && s.startsWith("#") == false
    }).map(p => {
      val split = p.split(":")
      (split(0) -> split(1))
    }).toMap
  }))

  /**
   * Gets the UNIX timestamp of the last successful save to disk.
   *
   * @return UNIX timestamp
   *
   * @since 1.0.0
   */
  def lastSave()(implicit opts: CommandOptions = DefaultCommandOptions): Long =
    send(LastSave)(asInteger)

  /**
   * Synchronously saves the dataset to disk.
   *
   * @since 1.0.0
   */
  def save()(implicit opts: CommandOptions = DefaultCommandOptions): Unit = send(Save)(asUnit)

  /**
   * Returns the current server time.
   *
   * @return pair of longs containing (1) UNIX timestamp and (2) microseconds
   *
   * @since 2.6.0
   */
  def time()(implicit opts: CommandOptions = DefaultCommandOptions): (Long, Long) = send(Time)(
    asMultiBulk[Long, Long, List, (Long, Long)](asBulk[Long, Long](flatten))(
      list => (list(0), list(1))
    )
  )

}