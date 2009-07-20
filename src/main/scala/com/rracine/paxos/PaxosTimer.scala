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
import scala.actors.Actor.loop

sealed case class TimerMessage ()

import java.util.TimerTask

// Reset ProgressTimer
case object PTReset  extends TimerMessage
// Reset ProgressTimer to 2x its remaining value.
case object PTReset2x extends TimerMessage
case class PTStop (tt: TimerTask) extends TimerMessage

/**
 * Sent by the Paxos Server to the PaxosTImer to starts periodic timer.
 * Sent by the PaxosTimer to the PaxosServer to cause a ViewChangeProof sendToAll.
 */
case object SendViewChangeProof extends TimerMessage

case object SendViewChange extends TimerMessage
case object StopViewChange extends TimerMessage

/**
 * Progress Timer to ensure the cluster is making progress.
 * Implemented as an Actor.
 * 
 * The progressTimerTask was marked as volatile, however, the Paxos Server is a single
 * threaded beast and the only thing which invokes ProgressTimer methods.
 * i.e., ProgressTimer never sees more than one thread externally.
 * 
 * The Timer thread affects both ProgressTimer and the PaxosServer state.
 * However, the Timer thread interacts with both via Actor messages.
 * Hopefully, leaving us safe from concurrency and parallism harm.
 *
 * The Paxos Server is an Actor processing messages on a single thread.
 * PaxosTimer runs a scheduled periodic VCProof timer task.
 * Upon task expiration the SendViewChangeProof message
 * is dropped on the Paxos Server message queue.
 */

object PaxosTimer extends AnyRef with Actor {
  
  import java.util.Timer
  import java.util.concurrent.atomic.AtomicInteger
  import java.lang.System.{currentTimeMillis => now}

  private val PT_DEFAULT_TIMEOUT = 15000  // millis
  private val VC_PROOF_PERIOD = 2000

  private var ptTimeout = PT_DEFAULT_TIMEOUT
  private val timer: Timer = new Timer ("PaxosTimer", false)

  /**
   * The task to be executed upon expiration.
   * The Option-al aspect is used to determine if the ProgressTimer is set.
   *  Some - ProgressTimer is set.
   *  None - ProgressTimer is not set.
   */
  private var progressTimerTask: Option[TimerTask] = None

  /**
   * Sends periodic ViewChange events back to Server until cancelled.
   */
  private var viewchangeTimerTask: Option[TimerTask] = None
  
  private def progressTimerStop (): Unit =
    progressTimerTask.foreach ((tt) => {
      progressTimerTask = None
      tt.cancel
      timer.purge
    })

  private def progressTimerReset (invoker: Actor, timeout: Long) = {
    progressTimerStop
    progressTimerTask = Some (new TimerTask {
      def run (): Unit = {
        PaxosTimer ! PTStop (this)
        invoker ! ProgressTimerExpiration
      }
    })
    timer.schedule (progressTimerTask.get, timeout)
  }

  private def stopViewChange () =
    viewchangeTimerTask.foreach ((tt) => {
      viewchangeTimerTask = None
      tt.cancel
      timer.purge
    })

  def act (): Unit =
    loop {
      react {

        case PTStop (ttexp)  => 
          progressTimerTask.foreach ((tt) => if (ttexp == tt) progressTimerStop)

        case PTReset => {          
          ptTimeout = PT_DEFAULT_TIMEOUT
          progressTimerReset (sender.receiver, PT_DEFAULT_TIMEOUT)
        }
        case PTReset2x => {
          ptTimeout = ptTimeout * 2
          progressTimerReset (sender.receiver, ptTimeout)
        }
       
        case SendViewChange => {
          stopViewChange
          val invoker = sender.receiver
          viewchangeTimerTask = Some (new TimerTask {
            def run (): Unit = {
              invoker ! SendViewChange
            }
          })
          timer.schedule (viewchangeTimerTask.get, VC_PROOF_PERIOD, VC_PROOF_PERIOD)
        }

        case StopViewChange => stopViewChange

        case SendViewChangeProof => {
          val invoker = sender.receiver
          timer.schedule (new TimerTask {
            def run (): Unit = {
              invoker ! SendViewChangeProof
            }
          }, VC_PROOF_PERIOD, VC_PROOF_PERIOD)
        }

      }
    }

  /**
   * Whether the ProgressTime has or has not been set.
   * This is the only method that is not invoked via an Actor message.
   * Therefore it is the only place where 2 threads have shared state.
   * This state is only mutated on the Actor thread and is read-only
   * to the invoker of isSet.
   *
   * Since the PaxosServer is a single thread beast and the ProgressTimer Actor
   * acts on the invoker thread (reacts and not receives) there is really only
   * one thread involved here.  The timerTask var doesn't?? need to be volatile.
   */
  def isSetProgressTimer (): Boolean = 
    progressTimerTask.isDefined

  start ()
}
