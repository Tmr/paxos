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

import scala.actors.Actor

trait Sender {

  def sendToAll (msg: Message): Unit

  def sendToLeader (msg: Message): Unit

  def replyToClient (): Unit

  def register (actor: Actor): Unit

}

object MC {
  val DEF_PORT = 6063
  val DEF_GROUP = "228.5.6.7"
}

object MsgId {
  val VC_ID: Byte     = 1
  val VCP_ID: Byte    = 2
  val PREP_ID: Byte   = 3
  val PREPOK_ID: Byte = 4
  val ACC_ID: Byte    = 5
  val PROP_ID: Byte   = 6
  val GO_ID: Byte     = 7
  val CU_ID: Byte     = 8
}

object MsgBuffer {

  import MsgId._
  import Paxos.{ARU, ServerId, View, Sequence, Command, TimeStamp, DataList}

  import java.nio.ByteBuffer

  type Buffer = ByteBuffer

  def allocate (sz: Int): Buffer = 
    ByteBuffer.allocate (sz)

  def deserialize (buffer: Array[Byte]): Message = {
    val bb = ByteBuffer.wrap (buffer)

    bb.get match {
      case `ACC_ID`    => Accept (bb.getInt, bb.getInt, bb.getInt)
      case `CU_ID`     => ClientUpdate (bb.getInt, bb.getInt, bb.getLong, "FIXME - NO COMMAND")
      case `VC_ID`     => ViewChange (bb.getInt, bb.getInt)
      case `VCP_ID`    => ViewChangeProof (bb.getInt, bb.getInt)
      case `PREP_ID`   => Prepare (bb.getInt, bb.getInt, bb.getInt)
      case `PREPOK_ID` => PrepareOK (bb.getInt, bb.getInt, bb.getInt, Nil)
      case `PROP_ID`   => Proposal (bb.getInt, bb.getInt, bb.getInt, "NO CMD")
      case b => throw new Exception ("Unknown message id: " + b)
      }
    }

  def serializeCmd (cmd: Paxos.Command, buffer: Buffer): Buffer = {
    cmd.foreach (buffer.putChar (_))
    buffer
  }

  def serialize (msg: ClientUpdate, buffer: Buffer): Buffer = {
    buffer.put (CU_ID).putInt (msg.clientId).putInt (msg.serverId).putLong (msg.ts)
    serializeCmd (msg.cmd, buffer)
  }

  def serialize (msg: ViewChange, buffer: Buffer): Buffer =
    buffer.put (VC_ID).putInt (msg.serverId).putInt (msg.attempted)

  def serialize (msg: ViewChangeProof, buffer:  Buffer): Buffer = 
    buffer.put (VCP_ID).putInt (msg.serverId).putInt (msg.installed)

  def serialize (msg: Prepare, buffer: Buffer): Buffer = 
    buffer.put (PREP_ID).putInt (msg.serverId).putInt (msg.view).putInt (msg.aru)

  def serialize (msg: PrepareOK, buffer: Buffer): Buffer =
    buffer.put (PREPOK_ID).putInt (msg.serverId).putInt (msg.leaderId).putInt (msg.view) // FIXME RPR data

  def serialize (msg: Accept, buffer: Buffer): Buffer = 
    buffer.put (ACC_ID).putInt (msg.serverId).putInt (msg.view).putInt (msg.seq)

  def serialize (msg: Proposal, buffer: Buffer): Buffer = {
    buffer.put (PROP_ID).putInt (msg.serverId).putInt (msg.view).putInt (msg.seq)
    serializeCmd (msg.cmd, buffer)
  }

  def serialize (msg: GloballyOrderedUpdate, buffer: Buffer): Buffer = {
    buffer.put (GO_ID).putInt (msg.serverId).putInt (msg.seq)
    serializeCmd (msg.cmd, buffer)
  }
}

/**
 * Sends and Receives Messages.
 * Messages are sent via normal method calls on the invoker's thread.
 * However inbound messages are on an Actor's thread and are placed
 * on the PaxosServer's Actor message queue for processing.
 */
class UDPMulticastCommunication () extends AnyRef with Sender {

  import MC.{DEF_PORT, DEF_GROUP}

  import java.net.{InetAddress, MulticastSocket, DatagramPacket}

  val BUFF_SZ = 128

  private val buffer = MsgBuffer.allocate (BUFF_SZ)

  val group = InetAddress.getByName (MC.DEF_GROUP)

  private var paxos: Actor = null

  private val socket = {
    val s = new MulticastSocket (MC.DEF_PORT)
    s.joinGroup (group)
    s
  }

  private val receiverThread = new Thread (new Runnable () {
    override def run (): Unit = {
      //val s = new MulticastSocket (MC.DEF_PORT)
      val buff = MsgBuffer.allocate (BUFF_SZ)
      //println ("Joining broadcast group")
      //s.joinGroup (group)
      while (true) {
        buff.clear
        val packet = new DatagramPacket (buff.array, buff.limit)
        socket.receive (packet)
        try {
          val msg = MsgBuffer.deserialize (packet.getData)
          paxos ! msg
        } catch {
          case e: Exception => println ("Deserialize failed: " + e.getMessage)
        }
      }
    }
  }, "Multicast Receiver")

  receiverThread.setDaemon (true)
  receiverThread.start  

  def serializeMsg (msg: Message, buffer: MsgBuffer.Buffer): MsgBuffer.Buffer =
    msg match {
      case m: ViewChange => MsgBuffer.serialize (m, buffer)
      case m: ViewChangeProof => MsgBuffer.serialize (m, buffer)
      case m: Prepare => MsgBuffer.serialize (m, buffer)
      case m: PrepareOK => MsgBuffer.serialize (m, buffer)
      case m: Proposal => MsgBuffer.serialize (m, buffer)
      case m: ClientUpdate => MsgBuffer.serialize (m, buffer)
      case m: Accept => MsgBuffer.serialize (m, buffer)
      case m: GloballyOrderedUpdate => MsgBuffer.serialize (m, buffer)
    }    
    
  def sendToAll (msg: Message): Unit =  {
    buffer.clear
    val buff = serializeMsg (msg, buffer)
    socket.send (new DatagramPacket(buff.array, buff.position, group, DEF_PORT))
  }

  def sendToLeader (msg: Message): Unit =
    sendToAll (msg)
  
  def replyToClient (): Unit =
    println ("Reply to client not implemented.")
  
  def register (actor: Actor) = 
    paxos = actor
}



