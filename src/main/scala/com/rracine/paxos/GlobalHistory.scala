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

import scala.collection.mutable.HashMap

import Paxos.Sequence

sealed abstract class GlobalHistoryEntry
case class GOProposal  (proposal: Proposal, accepts : HashMap[Paxos.ServerId, Accept]) extends GlobalHistoryEntry
case class GOConsensus (entry: GloballyOrderedUpdate) extends GlobalHistoryEntry

/**
 * Window sliding history of entries.
 * Constant time access.
 */

class GlobalHistory {

  /** Current size of the window */
  val windowSize: Int = 1024 // base 2 for efficient modulo operation
  
  /** The history window. */
  val window: Array[GlobalHistoryEntry] = new Array[GlobalHistoryEntry] (windowSize)
  
  /** Window base */
  var base = 0

  def isEmpty (seq: Sequence): Boolean =
    this (seq) == null

  def isOrdered (seq: Sequence): Boolean = 
    historyEntry (seq) match {
      case Some (e: GOConsensus) => true
      case Some (e: GOProposal) => false
      case None => false
    }

  def historyEntry (seq: Sequence): Option[GlobalHistoryEntry] = {
    val idx = seq - base
    if (idx < windowSize) {
      val entry = window (idx)
      if (entry == null) None else Some (window (idx))
    }
    else None
  }
  
  def hasEntry (seq: Sequence): Boolean =
    historyEntry (seq).isDefined

  def proposal (seq: Sequence): Option[GOProposal] =
    historyEntry (seq) match {
      case Some (gop: GOProposal) => Some (gop)
      case _ => None
    }

  def hasProposal (seq: Sequence): Boolean =
    historyEntry (seq) match {
      case Some (_: GOProposal) => true
      case _ => false
    }    

  /** Assure n slots are available. */
  def assure (n: Int): Unit = {
    var idx = n
    while (n > 0) {
      assert (isOrdered (base))
      base += 1
    }    
  }

  /** Access ith element of history */
  def apply (idx: Int): GlobalHistoryEntry = {
    assert (idx - base < windowSize)
    val entry = window (idx % windowSize)
    assert (entry != null)
    entry
  }

  def update (idx: Int, entry: GlobalHistoryEntry) = {
    assert (idx - base < windowSize)
    window (idx % windowSize) = entry
  }
}
