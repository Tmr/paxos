/*
 * Copyright 2009-2010 Ray Racine
 
 * This file is part of Crank.
 
 * Crank is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the LicenSe, or
 * (at your option) any later version.
 
 * Crank is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 
 * You should have received a copy of the GNU General Public License
 * along with Crank.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.rracine.paxos

import java.lang.System.{currentTimeMillis => now}

class TestCluster () extends AnyRef with Cluster {
  
  def replyToClient (): Unit = 
    println ("Reply to client - unfinished.")

  def nodeCount (): Int = 3
  
}

class PaxosServer (id: Int) extends Server (id, new TestCluster (), new UDPMulticastCommunication ()) {

  import Paxos.{ARU, ServerId, View, Sequence, Command, TimeStamp, DataList}

  def globalHistoryHasProposal (seq: Sequence): Boolean = {
    println ("Global history has proposal - unfinished.")
    false
  }

  def pendingUpdatesContains (cmd: Command): Boolean = {
    println ("Pending updates contains - unfinished.")
    false
  }

  def setUpdateTimer (id: ServerId): Unit = ()
    //println ("Set update timer - unfinished.")

  def cancelUpdateTimer (id: ServerId): Unit = ()

  def isBound (msg: ClientUpdate): Boolean = true

  def clearUpdateQueue (): Unit = 
    println ("Clear update queue - unfinished.")

  def pendingUpdatesRemove (cmd: Command): Boolean =  {
    println ("Pending updates removed - unfinished.")
    true
  }  
  
}

object Main {

  def sendClientMsgs (n: Int, server: Server): Unit = {
    var i = 0
    while (true) {
      Thread.sleep (1000,0)
      if (server.state != LEADER_ELECTION && server.myServerId == server.leaderId) {
        val s = "Client Msg: " + i
        println (s)
        i = i + 1
        val msg = ClientUpdate (server.myServerId * 100 + i, server.myServerId, now, s)
        server.clientUpdateHandler (msg)
      }
    }
  }
    
  def parseServerId (args: Array[String]) = args (0).toInt

  def main (args: Array[String]): Unit = {
    try {
      val server = new PaxosServer (parseServerId (args))
      println ("Starting Paxos Server - " + server.myServerId)
      server.start
      Thread.sleep (1000, 0) // yield (can't yield!!)
      server ! InitServer
      sendClientMsgs (100, server)
    } catch {
      case e => {
        e.printStackTrace
      }
    }
  }

}
