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
import scala.collection.mutable.{ArrayBuffer, HashMap, Queue}

trait Cluster {

  /** Number of nodes in the cluster */
  def nodeCount (): Int

  /** Count of nodes for a quorum. */
  def quorum (): Int = peerQuorum + 1

  /**
   * Number of peers necessary to make a quorum.
   * This is because this node counts itself.
   */
  def peerQuorum (): Int = nodeCount / 2

  def replyToClient (): Unit
}

object Paxos {

  // Unique server identifier.
  type ServerId = Int

  // The current view identifier.
  type View = Int

  type Sequence = Int

  // The synchronized command 
  type Command = String

  type TimeStamp = Long
  val MIN_TS: TimeStamp = 0
  val MAX_TS: TimeStamp = Math.MAX_LONG

  // List of Proposals and GlobalOrderUpdates
  type DataList = List[Message]

  //
  type ARU = Int
}

import Paxos.{ARU, ServerId, View, Sequence, Command, TimeStamp, DataList}


sealed case class Message ()

case object InitServer
case object ProgressTimerExpiration

case class ClientUpdate (clientId: ServerId, serverId: ServerId, ts: TimeStamp, cmd: Command) extends Message
case class ViewChange (serverId: ServerId, attempted: View) extends Message
case class ViewChangeProof (serverId: ServerId, installed: View) extends Message
case class Prepare (serverId: ServerId, view: View, aru: ARU) extends Message
case class PrepareOK (serverId: ServerId, leaderId: ServerId, view: View, data: Paxos.DataList) extends Message
case class Proposal (serverId: ServerId, view: View, seq: Sequence, cmd: Command) extends Message
case class Accept (serverId: ServerId, view: View, seq: Sequence) extends Message
case class GloballyOrderedUpdate (serverId: ServerId, seq: Sequence, cmd: Command) extends Message

sealed case class State ()
case object LEADER_ELECTION extends State
case object REG_LEADER extends State
case object REG_NONLEADER extends State

import scala.actors.Actor
import scala.actors.Actor.loop

abstract class Server (val myServerId: ServerId, cluster: Cluster, send: Sender) extends AnyRef with Actor {

  /* View related state. */
  var lastAttempted: View = _
  var lastInstalled: View = _
  val viewChanges: HashMap[ServerId, ViewChange] = HashMap.empty
  
  var state: State = LEADER_ELECTION
  val prepareOKs: Array[Option[PrepareOK]] = Array.make[Option[PrepareOK]] (cluster.nodeCount, None)

  val globalHistory = new GlobalHistory ()

  val lastEnqueued: HashMap[ServerId, TimeStamp] = HashMap.empty

  // Global ordering variables
  var lastProposed: Sequence = _
  var localARU: ARU = _

  val lastExecuted: HashMap[ServerId, TimeStamp] = HashMap.empty

  val updateQueue: Queue[ClientUpdate] = new Queue ()
  
  /** Client update messages indexed by the client id. */
  val pendingUpdates: HashMap[ServerId, ArrayBuffer[ClientUpdate]] = HashMap.empty

  def leaderId (): ServerId = 
    lastInstalled % cluster.nodeCount

  def check (msg: Message, reason: String, bool: Boolean): Unit = 
    if (bool) println ("  Message conflict for " + msg + ". Reason: " + reason)

  def conflictViewChange (msg: ViewChange): Boolean = {
    check (msg, "Self message", msg.serverId == myServerId)
    check (msg, "Not in LeaderElection", state != LEADER_ELECTION)
    check (msg, "PT is set", PaxosTimer.isSetProgressTimer)
    check (msg, "Attempt is less then Last Installed.", msg.attempted <= lastInstalled)    
    msg.serverId == myServerId || state != LEADER_ELECTION || PaxosTimer.isSetProgressTimer || msg.attempted <= lastInstalled
  }

  /**
   * Check incoming messages.  All conflicted messages are discarded.
   */
  def messageConflict (msg: Message): Boolean = 
    msg match {
      // Below from paper.  Problem is for a Cluster of 1 our VC msg is never processed.
      case msg @ ViewChange (serverId, attempted) => conflictViewChange (msg)
      case ViewChangeProof (serverId, _) => serverId == myServerId || state != LEADER_ELECTION
      case Prepare (serverId, view, aru) => serverId == myServerId || view != lastAttempted
      case PrepareOK (serverId, leaderId, view, _) => leaderId != myServerId || serverId == myServerId || state != LEADER_ELECTION || view != lastAttempted
      case Proposal (serverId, view, _, update) => serverId == myServerId || state != REG_NONLEADER || view != lastInstalled
      case Accept (serverId, view, seq) => serverId == myServerId || view != lastInstalled || globalHistory.hasProposal (seq)
      case _ => false
    }

  /**
   * Track number of peers who agree on the new view.
   * Returns whether the VC was applied or not
   * since multiple VCs can be received because of
   * rebroadcast for msg loss and timing considerations
   */ 
  def updateViewChange (msg: ViewChange): Boolean =
    if (viewChanges contains msg.serverId)
      false 
    else {
      viewChanges update (msg.serverId, msg)
      true
    }

  /**
   * Generic Global Order History Data Structure Mutation Method
   * The following mutate global state based upon the message being processed.
   * Pattern match and dispatch to allow recursive processing of messages.
   */
  def updateDataStructures (msg: Message): Unit =     
    msg match {

      case msg @ PrepareOK (serverId, _, _, data) => 
        if (prepareOKs (serverId).isEmpty) {
          prepareOKs (serverId) = Some (msg)
          for (e <- data) {
            updateDataStructures (e)
          }
        }
      
      case pnew @ Proposal (_, view, seq, _) =>
        globalHistory.proposal (seq).foreach ((gop) =>          
            if (view > gop.proposal.view)
              globalHistory (seq) = GOProposal (pnew, HashMap.empty))

      case msg @ Accept (serverId, _, seq) =>
        globalHistory.proposal (seq).foreach ((gop) =>           
            if ((gop.accepts.size < cluster.peerQuorum) && (!gop.accepts.contains (serverId))) {
              gop.accepts (serverId) = msg
            })                                            
      
      case msg @ GloballyOrderedUpdate (_, seq, _) =>
        if (!globalHistory.isOrdered (seq))
          globalHistory.update (seq, GOConsensus (msg))

      case msg =>  println ("ERROR: Misapplying a message to update global data structures: " + msg)
    }

  /**
   * Process a received ViewChange message.
   * If the ViewChange is higher then our last attempt
   * move to the higher View and enter LeaderElection.
   *
   * If the view is equal to what the server is attempting
   * then count that server as part of the quorum for the
   * new view.
   *
   * If a quorum of servers have agreed on the new view
   * shift to the prepare phase.
   */
  def processViewChange (vc: ViewChange): Unit = {
    if (vc.attempted > lastAttempted && !PaxosTimer.isSetProgressTimer) {
      shiftToLeaderElection (vc.attempted)
      updateViewChange (vc)
    } else if (vc.attempted == lastAttempted && updateViewChange (vc) && viewPreinstallReady (vc.attempted)) {
        PaxosTimer ! PTReset2x  
        if (leaderOfView (lastAttempted))
          shiftToPreparePhase
      }
    }

  /**
   * A ViewChangeProof means a majority outside of ourselves
   * have agreed on a new view already.
   * Go ahead and join the cluster under that view.
   */
  def processViewChangeProof (msg: ViewChangeProof): Unit =
    if (msg.installed > lastInstalled) {
      lastAttempted = msg.installed
      if (leaderOfView (lastAttempted))
        shiftToPreparePhase 
      else shiftToRegNonLeader
    }
  
  /**
   * Process a received Prepare message.
   * If we are still electing give it up.
   * A prepare means a majority elected a leader so go ahead and move to out of election
   * and respond that we are ready for the new leader.
   */
  def processPrepare (msg: Prepare): Unit = 
    if (state == LEADER_ELECTION) {
      val datalist = constructDataList (msg.aru)
      val okMsg = PrepareOK (myServerId, msg.serverId, msg.view, datalist)
      prepareOKs (myServerId) = Some (okMsg)
      shiftToRegNonLeader 
      send.sendToLeader (okMsg)
    } else prepareOKs (myServerId).map ((msg) => send.sendToLeader (msg))
  
  /**
   * Process a received PrepareOK message.
   * If a majority agree become the leader.
   *
   * If we fail to get elected leader our
   * ProgressTimer will expire and back to
   * leader election we go.
   */
  def processPrepareOK (msg: PrepareOK): Unit = {
      updateDataStructures (msg)
      if (viewPreparedReady (msg.view))
        shiftToRegLeader
  }

  def processAccept (msg: Accept): Unit = {
    updateDataStructures (msg)
    globallyOrderedReady (msg.seq).foreach ((gop) => {
      updateDataStructures (GloballyOrderedUpdate (gop.proposal.serverId, 
                                                   msg.seq, 
                                                   gop.proposal.cmd))
    })
    advanceARU
  }

  def processProposal (msg: Proposal) = {
    updateDataStructures (msg)
    val accept = Accept (myServerId, msg.view, msg.seq)
    syncToDisk
    send.sendToAll (accept)
  }

  def advanceARU (): Unit = {
    var idx = localARU + 1
    while (true) {
      if (globalHistory.isOrdered (idx)) {
        localARU += 1
        idx += 1
      } else return
    }
  }

  def sendProposal (): Unit = {
    val seq = lastProposed + 1

    if (globalHistory.isOrdered (seq)) {
      lastProposed += 1
      sendProposal   // Scala is tail recursive here.
    } else {         // Different from paper added after email with J.K.
      val cmd = globalHistory.proposal (seq) match {
        case Some (gop: GOProposal) => gop.proposal.cmd
        case None => 
          if (updateQueue.isEmpty) 
            return 
          else updateQueue.dequeue.cmd
      }
      
      // Paper has 'view', should be lastInstalled. Confirmed with J.K.
      val proposal = Proposal (myServerId, lastInstalled, seq, cmd)
      updateDataStructures (proposal)
      lastProposed = seq
      syncToDisk
      send.sendToAll (proposal)
    }
  }
    
  /**
   * A proposal is ready for global ordering if a majority of the
   * cluster has accepted the proposal.
   */
  def globallyOrderedReady (seq: Sequence): Option[GOProposal] =
    globalHistory.proposal (seq).filter (_.accepts.size >= cluster.peerQuorum)

  def postProcessClientUpdate (msg: ClientUpdate): Unit = {
    advanceARU
    if (msg.serverId == myServerId) {
      cluster.replyToClient 
      if (pendingUpdates.contains (msg.clientId)) {
        cancelUpdateTimer (msg.clientId)
        pendingUpdates - (msg.clientId)
      }
    }
    lastExecuted (msg.clientId) = now 
    if (state != LEADER_ELECTION)
      PaxosTimer ! PTReset
    if (state == REG_LEADER)
      sendProposal
  }

  def leaderOfView (view: View): Boolean = 
    myServerId == view % cluster.nodeCount
  
  /** Callback method invoked on progress timer expiration. */
  def progressTimerExpiration (): Unit = {
    println ("Progress Timer expiration: shifting to leader election.")
    shiftToLeaderElection (lastAttempted + 1)
  }

  /* Start ViewChangeProof periodic timer */
  def startVCProofTimer (): Unit =     
    PaxosTimer ! SendViewChangeProof

  /**
   * Progress has not been made.
   * Prepare and elect a new leader.
   */
  def shiftToLeaderElection (view: View): Unit = {
    state = LEADER_ELECTION
    viewChanges.clear; lastEnqueued.clear; lastAttempted = view

    // clear prepareOKs
    for (i <- 0 until prepareOKs.size) {
      prepareOKs (i) = None
    }

    val vc = ViewChange (myServerId, lastAttempted)
    send.sendToAll (vc)
    updateViewChange (vc)
    println ("Starting Periodic VC")
    PaxosTimer ! SendViewChange
  }

  def cancelUpdateTimer (clientId: ServerId): Unit

  def syncToDisk (): Unit = ()

  def pendingUpdatesContains (cmd: Command): Boolean

  def pendingUpdatesRemove (cmd: Command): Boolean

  def shiftToRegLeader (): Unit = {
    state = REG_LEADER
    PaxosTimer ! StopViewChange
    PaxosTimer ! PTReset
    enqueueUnboundPendingUpdates
    removeBoundUpdatesFromQueue
    lastProposed = localARU
    sendProposal
  }

  def clearUpdateQueue (): Unit
  
  def shiftToRegNonLeader (): Unit = {
    state = REG_NONLEADER
    lastInstalled = lastAttempted
    PaxosTimer ! StopViewChange
    PaxosTimer ! PTReset
    clearUpdateQueue
    syncToDisk
  }

  def clearLastEnqueued (): Unit = 
    lastEnqueued.clear
  
  /** Fix Me - Figure 8 */
  def constructDataList (aru: ARU) = Nil

  var preparedCount = 1

  def viewPreparedReady (view: View): Boolean = {
    val quorum = cluster.quorum
    var preparedCount = 0

    for (pokOpt <- prepareOKs) {
      pokOpt.map ((pok) => if (pok.view == view) preparedCount += 1)
      if (preparedCount == quorum) return true
    }
    return false
  }

  /**
   * Ready to pre-install a view of a majority of the cluster agree on a new view.
   */
  def viewPreinstallReady (view: View): Boolean = 
    viewChanges.size >= cluster.quorum
  
  def shiftToPreparePhase (): Unit = {
    lastInstalled = lastAttempted
    val prepare = Prepare (myServerId, lastInstalled, localARU)
    val datalist = constructDataList (localARU)
    val prepareOK = PrepareOK (myServerId, myServerId, lastInstalled, datalist)
    prepareOKs (myServerId) = Some (prepareOK)
    clearLastEnqueued
    syncToDisk
    send.sendToAll (prepare)
  }

  /*=======================
   * Client Update Handling
   *======================= */
  def clientUpdateHandler (msg: ClientUpdate): Unit = {
    state match {
      case LEADER_ELECTION =>
        if (msg.serverId != myServerId && enqueueUpdate (msg)) {
          println ("CU - LE")          
          //addToPendingUpdates (msg) All these temp removed
        }

      case REG_NONLEADER =>
        if (msg.serverId == myServerId) {
          //addToPendingUpdates (msg)
          println ("CU - RN")
          send.sendToLeader (msg)
          ()
        }
      
      case REG_LEADER =>
        if (enqueueUpdate (msg)) {
          //if (msg.serverId == myServerId)
          //  addToPendingUpdates (msg)
          println ("CU - RL")
          sendProposal
        }
      }
    }

  // FIXME
  def isBound (update: ClientUpdate): Boolean

  def addToPendingUpdates (msg: ClientUpdate): Unit = {
    // FIXME FOR LOOKUP FAILS
    //pendingUpdates (msg.clientId) += msg
    setUpdateTimer (msg.clientId)
    syncToDisk
  }

  def setUpdateTimer (clientId: ServerId): Unit 
  
  def enqueueUpdate (msg: ClientUpdate): Boolean =
    if (msg.ts <= lastExecuted.getOrElse (msg.clientId, Paxos.MIN_TS) || 
        msg.ts <= lastEnqueued.getOrElse (msg.clientId, Paxos.MIN_TS))
      false
    else {
      updateQueue += msg
      lastEnqueued.update (msg.clientId, msg.ts)
      true
    }

  def enqueueUnboundPendingUpdates (): Unit =
    for ((_,buffer) <- pendingUpdates) {
      for (u <- buffer) {
        if (!isBound (u) || !updateQueue.contains (u))
          enqueueUpdate (u)
      }
    }

  def removeBoundUpdatesFromQueue (): Unit =
    updateQueue.dequeueAll ((u: ClientUpdate) => 
      if (isBound (u) || (u.ts <= lastExecuted.getOrElse (u.clientId, Paxos.MIN_TS)) ||
          (u.ts <= lastEnqueued.getOrElse (u.clientId, Paxos.MIN_TS) && u.serverId != myServerId)) {
            if (u.ts > lastEnqueued.getOrElse (u.clientId, Paxos.MIN_TS))
              lastEnqueued (u.clientId) = u.ts
            true
          } else false)
  
  /**
   * The heart of the Paxos Server.  Process msgs off msg queue.
   * An aspect of processing a message is mutation of state.
   * All state mutating message processing is encapsulated by
   * the updateDataStructures driver method.
   */
  def act (): Unit = {
    loop {
      println ("State: " + state + " lastInstalled: " + lastInstalled + " lastAttempted: " + lastAttempted)      
      react {
        case SendViewChangeProof =>
          send.sendToAll (ViewChangeProof (myServerId, lastInstalled))

        case SendViewChange => {
          val msg = ViewChange (myServerId, lastAttempted)
          println ("Sending periodic ViewChange: " + msg)
          send.sendToAll (msg)
        }

        case msg: Message => { // nested for msg conflict handling
          println ("Msg: " + msg)
          if (!messageConflict (msg))
            msg match {
              case msg: Accept => processAccept (msg)
              case msg: ViewChange => processViewChange (msg)
              case msg: ViewChangeProof => processViewChangeProof (msg)
              case msg: Prepare => processPrepare (msg)
              case msg: PrepareOK => processPrepareOK (msg)
              case msg: ClientUpdate => clientUpdateHandler (msg)
              case msg: Proposal => processProposal (msg)
              case msg @ _ => println ("Unhandled Message: " + msg.toString)
            } else println (" -- Dropped --")
          }

        case ProgressTimerExpiration => {
          progressTimerExpiration
        }        

        case InitServer => {
          progressTimerExpiration
          startVCProofTimer
        }
        case msg @ _ => println ("Unknown msg: " + msg)
      }
    }
  }

  send.register (this)
}
