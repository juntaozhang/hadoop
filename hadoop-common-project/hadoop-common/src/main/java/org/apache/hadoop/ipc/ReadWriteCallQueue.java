/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ipc;

import java.lang.ref.WeakReference;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nonnull;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.util.MBeans;

/**
 * A call queue that splits RPC calls into separate read and write sub-queues
 * and schedules them in batches.
 *
 * <p>Concurrency follows the same lightweight approach as
 * {@link FairCallQueue}: a {@link Semaphore} ({@link #count}) tracks the number
 * of queued elements and doubles as the not-empty signal, while a second
 * {@link Semaphore} ({@link #slots}) enforces the total capacity and provides
 * the not-full blocking behavior. Because producers only touch the semaphores
 * and the (thread-safe) sub-queues, no lock is held while another lock is
 * acquired, so there is no nested-lock ordering hazard.
 *
 * <p>The total capacity is enforced by {@link #slots}; the two sub-queues
 * themselves are <em>unbounded</em>.
 * This guarantees that a burst of read calls can never starve write calls of
 * capacity (and vice versa): either side may grow up to the shared total
 * capacity.
 * Classification is done via {@link Schedulable#isWrite()}.
 *
 * <p>Scheduling is batch based: each cycle drains up to {@code readBatch}
 * reads followed by up to {@code writeQuota} writes. This groups reads
 * together so that multiple handlers can hold shared resources (such as the
 * NameNode read lock) concurrently. If one sub-queue empties before its batch
 * is complete, the queue immediately falls back to the other sub-queue so an
 * idle side never causes the busy side to wait.
 *
 * <p>The batch scheduler state ({@link #phase}, {@link #readsRemaining},
 * {@link #writesRemaining}) is shared mutable state read and written only by
 * consumers, so it is guarded by a single {@link #schedLock}. Producers never
 * acquire this lock.
 */
public class ReadWriteCallQueue<E extends Schedulable> extends AbstractQueue<E>
    implements BlockingQueue<E> {

  public static final String READ_BATCH_KEY = "readwrite.read.batch";
  public static final String WRITE_QUOTA_KEY = "readwrite.write.quota";

  public static final int READ_BATCH_DEFAULT = 128;
  public static final int WRITE_QUOTA_DEFAULT = 32;

  /** Total capacity shared by both sub-queues. */
  private final int capacity;

  /**
   * Permits equal the number of elements currently queued. Producers release a
   * permit on insert; consumers acquire one before extracting. Also serves as
   * the not-empty signal and the basis for {@link #size()}, mirroring
   * {@link FairCallQueue}'s semaphore design.
   */
  private final Semaphore count = new Semaphore(0);

  /**
   * Permits equal the number of free slots. Producers acquire a permit before
   * inserting (blocking when the queue is full); consumers release a permit
   * when
   * an element is removed. The invariant
   * {@code size() + remainingCapacity() == capacity} always holds.
   */
  private final Semaphore slots;

  /** Unbounded sub-queues. Total capacity is tracked by {@link #slots}. */
  private final BlockingQueue<E> readQueue;

  private final BlockingQueue<E> writeQueue;

  /**
   * Guards the batch scheduler state mutated by {@link #extract()}. Only
   * consumers acquire this lock, and never while holding any other lock, so it
   * cannot participate in a lock-ordering deadlock.
   */
  private final ReentrantLock schedLock = new ReentrantLock();

  private enum Phase {
    READ, WRITE
  }

  private final int readBatch;
  private final int writeQuota;
  private Phase phase = Phase.READ;
  private int readsRemaining;
  private int writesRemaining;

  private final AtomicLong readPhaseCount = new AtomicLong(0);
  private final AtomicLong writePhaseCount = new AtomicLong(0);

  /**
   * Create a ReadWriteCallQueue.
   *
   * @param priorityLevels unused, kept for parity with other call queues
   * @param capacity the total capacity shared by the read and write sub-queues
   * @param ns the configuration namespace (e.g. "ipc.8020")
   * @param conf the configuration to read batch sizes from
   */
  public ReadWriteCallQueue(int priorityLevels, int capacity, String ns,
      Configuration conf) {
    if (capacity <= 0) {
      throw new IllegalArgumentException("capacity must be positive");
    }
    this.capacity = capacity;
    this.slots = new Semaphore(capacity);

    this.readQueue = new LinkedBlockingQueue<>();
    this.writeQueue = new LinkedBlockingQueue<>();

    this.readBatch = Math.max(1,
        conf.getInt(ns + "." + READ_BATCH_KEY, READ_BATCH_DEFAULT));
    this.writeQuota = Math.max(1,
        conf.getInt(ns + "." + WRITE_QUOTA_KEY, WRITE_QUOTA_DEFAULT));

    this.readsRemaining = readBatch;
    this.writesRemaining = writeQuota;
    this.readPhaseCount.incrementAndGet();

    // Make this the active source of metrics
    MetricsProxy mp = MetricsProxy.getInstance(ns);
    mp.setDelegate(this);
  }

  // ------------------- enqueue (under slots semaphore) -------------------

  @Override
  public boolean offer(@Nonnull E e) {
    Objects.requireNonNull(e);
    if (!slots.tryAcquire()) {
      return false;
    }
    insert(e);
    count.release();
    return true;
  }

  @Override
  public void put(@Nonnull E e) throws InterruptedException {
    Objects.requireNonNull(e);
    slots.acquire();
    insert(e);
    count.release();
  }

  @Override
  public boolean offer(@Nonnull E e, long timeout, @Nonnull TimeUnit unit)
      throws InterruptedException {
    Objects.requireNonNull(e);
    if (!slots.tryAcquire(timeout, unit)) {
      return false;
    }
    insert(e);
    count.release();
    return true;
  }

  /**
   * Overridden (with an annotated parameter) so the inherited
   * {@link AbstractQueue#add} is not flagged for relaxing the non-null element
   * contract of {@link BlockingQueue#add}. Throws if the queue is full.
   */
  @Override
  public boolean add(@Nonnull E e) {
    if (!offer(e)) {
      throw new IllegalStateException("Queue full");
    }
    return true;
  }

  /** Route the element to its sub-queue. Sub-queues are unbounded. */
  private void insert(E e) {
    if (e.isWrite()) {
      writeQueue.add(e);
    } else {
      readQueue.add(e);
    }
  }

  // ------------------- dequeue (under schedLock, slots/count semaphores) --

  @Override
  public E poll() {
    if (!count.tryAcquire()) {
      return null;
    }
    try {
      return extract();
    } finally {
      slots.release();
    }
  }

  @Nonnull
  @Override
  public E take() throws InterruptedException {
    count.acquire();
    try {
      return extract();
    } finally {
      slots.release();
    }
  }

  @Override
  public E poll(long timeout, @Nonnull TimeUnit unit)
      throws InterruptedException {
    if (!count.tryAcquire(timeout, unit)) {
      return null;
    }
    try {
      return extract();
    } finally {
      slots.release();
    }
  }

  /**
   * Pull the next element honoring the configured batch sizes.
   * Each permit is released by a producer strictly after
   * its element is inserted, so a permit always corresponds to a real element;
   * this method therefore always returns a non-null element.
   */
  @Nonnull
  private E extract() {
    schedLock.lock();
    try {
      for (int i = 0; i < 2; i++) {
        if (phase == Phase.READ) {
          E e = readQueue.poll();
          if (e != null) {
            readsRemaining--;
            if (readsRemaining == 0) {
              switchToWritePhase();
            }
            return e;
          }
          switchToWritePhase();
        } else {
          E e = writeQueue.poll();
          if (e != null) {
            writesRemaining--;
            if (writesRemaining == 0) {
              switchToReadPhase();
            }
            return e;
          }
          switchToReadPhase();
        }
      }
      throw new IllegalStateException(
          "extract() found no element despite holding a count permit");
    } finally {
      schedLock.unlock();
    }
  }

  private void switchToReadPhase() {
    phase = Phase.READ;
    readsRemaining = readBatch;
    readPhaseCount.incrementAndGet();
  }

  private void switchToWritePhase() {
    phase = Phase.WRITE;
    writesRemaining = writeQuota;
    writePhaseCount.incrementAndGet();
  }

  // ------------------- queries -------------------

  /**
   * Returns the head of the write sub-queue if present, otherwise the head of
   * the read sub-queue. This does not reflect the batch-based scheduling
   * order; it is only a best-effort inspection.
   */
  @Override
  public E peek() {
    E write = writeQueue.peek();
    if (write != null) {
      return write;
    }
    return readQueue.peek();
  }

  @Override
  public int size() {
    return count.availablePermits();
  }

  @Override
  public int remainingCapacity() {
    return slots.availablePermits();
  }

  @Override
  public boolean contains(Object o) {
    return writeQueue.contains(o) || readQueue.contains(o);
  }

  @Override
  public Iterator<E> iterator() {
    throw new NotImplementedException();
  }

  /**
   * Drains elements from this queue into the given collection. The drained
   * order follows the batch-based scheduler while the current batch lasts,
   * but may fall back to the non-empty side when one sub-queue is empty.
   */
  @Override
  public int drainTo(@Nonnull Collection<? super E> c) {
    return drainTo(c, Integer.MAX_VALUE);
  }

  /**
   * Drains up to {@code maxElements} elements from this queue into the given
   * collection. See {@link #drainTo(Collection)} for ordering notes.
   */
  @Override
  public int drainTo(@Nonnull Collection<? super E> c, int maxElements) {
    if (c == this) {
      throw new IllegalArgumentException();
    }
    if (maxElements <= 0) {
      return 0;
    }
    int n = 0;
    while (n < maxElements && count.tryAcquire()) {
      E x = extract();
      n++;
      // Release the slot for the removed element whether or not c.add()
      // succeeds,
      // so a rejecting destination cannot leak capacity: the size/capacity
      // invariant stays correct even if the element is ultimately lost.
      try {
        c.add(x);
      } finally {
        slots.release();
      }
    }
    return n;
  }

  @Override
  public E remove() {
    E x = poll();
    if (x == null) {
      throw new java.util.NoSuchElementException();
    }
    return x;
  }

  @Override
  public E element() {
    E x = peek();
    if (x == null) {
      throw new java.util.NoSuchElementException();
    }
    return x;
  }

  // ------------------- metrics -------------------

  public int getReadQueueSize() {
    return readQueue.size();
  }

  public int getWriteQueueSize() {
    return writeQueue.size();
  }

  public long getReadPhaseCount() {
    return readPhaseCount.get();
  }

  public long getWritePhaseCount() {
    return writePhaseCount.get();
  }

  /**
   * MetricsProxy is a singleton because we may init multiple
   * ReadWriteCallQueues, but the metrics system cannot unregister beans
   * cleanly.
   */
  private static final class MetricsProxy implements ReadWriteCallQueueMXBean {
    // One singleton per namespace
    private static final HashMap<String, MetricsProxy> INSTANCES =
        new HashMap<String, MetricsProxy>();

    // Weakref for delegate, so we don't retain it forever if it can be GC'd
    private WeakReference<ReadWriteCallQueue<? extends Schedulable>> delegate;

    // Keep track of how many objects we registered
    private int revisionNumber = 0;

    private MetricsProxy(String namespace) {
      MBeans.register(namespace, "ReadWriteCallQueue", this);
    }

    public static synchronized MetricsProxy getInstance(String namespace) {
      MetricsProxy mp = INSTANCES.get(namespace);
      if (mp == null) {
        // We must create one
        mp = new MetricsProxy(namespace);
        INSTANCES.put(namespace, mp);
      }
      return mp;
    }

    public void setDelegate(ReadWriteCallQueue<? extends Schedulable> obj) {
      this.delegate =
          new WeakReference<ReadWriteCallQueue<? extends Schedulable>>(obj);
      this.revisionNumber++;
    }

    @Override
    public int getReadQueueSize() {
      ReadWriteCallQueue<? extends Schedulable> obj = this.delegate.get();
      if (obj == null) {
        return 0;
      }
      return obj.getReadQueueSize();
    }

    @Override
    public int getWriteQueueSize() {
      ReadWriteCallQueue<? extends Schedulable> obj = this.delegate.get();
      if (obj == null) {
        return 0;
      }
      return obj.getWriteQueueSize();
    }

    @Override
    public long getReadPhaseCount() {
      ReadWriteCallQueue<? extends Schedulable> obj = this.delegate.get();
      if (obj == null) {
        return 0;
      }
      return obj.getReadPhaseCount();
    }

    @Override
    public long getWritePhaseCount() {
      ReadWriteCallQueue<? extends Schedulable> obj = this.delegate.get();
      if (obj == null) {
        return 0;
      }
      return obj.getWritePhaseCount();
    }

    @Override
    public int getRevision() {
      return revisionNumber;
    }
  }
}
