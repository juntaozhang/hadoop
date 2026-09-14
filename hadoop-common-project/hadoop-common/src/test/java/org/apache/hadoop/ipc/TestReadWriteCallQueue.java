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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.management.ManagementFactory;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;

/** Tests for {@link ReadWriteCallQueue}. */
public class TestReadWriteCallQueue {

  private ReadWriteCallQueue<Schedulable> rwq;

  private Schedulable mockCall(String id, boolean isWrite) {
    Schedulable call = mock(Schedulable.class);
    UserGroupInformation ugi = mock(UserGroupInformation.class);
    when(ugi.getUserName()).thenReturn(id);
    when(call.getUserGroupInformation()).thenReturn(ugi);
    when(call.getPriorityLevel()).thenReturn(0);
    when(call.isWrite()).thenReturn(isWrite);
    when(call.toString()).thenReturn("id=" + id + " write=" + isWrite);
    return call;
  }

  private Schedulable mockCall(String id) {
    return mockCall(id, false);
  }

  @Before
  public void setUp() {
    Configuration conf = new Configuration();
    rwq = new ReadWriteCallQueue<Schedulable>(1, 10, "ns", conf);
  }

  // ------------------- capacity / routing -------------------

  @Test
  public void testInitialRemainingCapacity() {
    assertEquals(10, rwq.remainingCapacity());
  }

  @Test
  public void testOfferSucceeds() {
    for (int i = 0; i < 5; i++) {
      assertTrue(rwq.offer(mockCall("c")));
    }
    assertEquals(5, rwq.size());
    assertEquals(5, rwq.remainingCapacity());
  }

  @Test
  public void testOfferFailsWhenFull() {
    for (int i = 0; i < 10; i++) {
      assertTrue(rwq.offer(mockCall("c")));
    }
    assertFalse(rwq.offer(mockCall("c"))); // full
    assertEquals(10, rwq.size());
    assertEquals(0, rwq.remainingCapacity());
  }

  /**
   * Sub-queues are unbounded, so a burst of read calls may consume the entire
   * shared capacity without starving on a per-side quota.
   */
  @Test
  public void testUnboundedSubqueueCanFillTotalCapacity() {
    for (int i = 0; i < 10; i++) {
      assertTrue(rwq.offer(mockCall("r" + i, false)));
    }
    assertEquals(10, rwq.size());
    assertFalse(rwq.offer(mockCall("overflow", false)));
  }

  @Test
  public void testPeekNonDestructive() {
    Schedulable call = mockCall("c", false);
    assertTrue(rwq.offer(call));
    assertEquals(call, rwq.peek());
    assertEquals(call, rwq.peek());
    assertEquals(1, rwq.size());
  }

  @Test
  public void testPollReturnsNullWhenEmpty() {
    assertNull(rwq.poll());
  }

  @Test
  public void testPollRemoves() {
    Schedulable call = mockCall("c", false);
    assertTrue(rwq.offer(call));
    assertEquals(call, rwq.poll());
    assertEquals(0, rwq.size());
  }

  @Test
  public void testPollTimeout() throws InterruptedException {
    assertNull(rwq.poll(10, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testPollSuccessWithTimeout() throws InterruptedException {
    Schedulable call = mockCall("c", false);
    assertTrue(rwq.offer(call));
    assertEquals(call, rwq.poll(10, TimeUnit.MILLISECONDS));
    assertEquals(0, rwq.size());
  }

  @Test
  public void testOfferTimeout() throws InterruptedException {
    for (int i = 0; i < 10; i++) {
      assertTrue(rwq.offer(mockCall("c"), 10, TimeUnit.MILLISECONDS));
    }
    assertFalse(rwq.offer(mockCall("e"), 10, TimeUnit.MILLISECONDS)); // full
    assertEquals(10, rwq.size());
  }

  // ------------------- scheduling -------------------

  /**
   * With readBatch=3 and writeQuota=1, reads are drained in runs of three
   * followed by one write.
   */
  @Test
  public void testReadBatchThenWriteQuota() throws InterruptedException {
    Configuration conf = new Configuration();
    conf.setInt("ns." + ReadWriteCallQueue.READ_BATCH_KEY, 3);
    conf.setInt("ns." + ReadWriteCallQueue.WRITE_QUOTA_KEY, 1);
    ReadWriteCallQueue<Schedulable> q =
        new ReadWriteCallQueue<Schedulable>(1, 100, "ns", conf);

    Schedulable r1 = mockCall("r1", false);
    Schedulable r2 = mockCall("r2", false);
    Schedulable r3 = mockCall("r3", false);
    Schedulable w1 = mockCall("w1", true);
    Schedulable r4 = mockCall("r4", false);
    Schedulable r5 = mockCall("r5", false);
    Schedulable r6 = mockCall("r6", false);
    Schedulable w2 = mockCall("w2", true);
    q.put(r1);
    q.put(r2);
    q.put(r3);
    q.put(w1);
    q.put(r4);
    q.put(r5);
    q.put(r6);
    q.put(w2);

    assertEquals(r1, q.take());
    assertEquals(r2, q.take());
    assertEquals(r3, q.take());
    assertEquals(w1, q.take());
    assertEquals(r4, q.take());
    assertEquals(r5, q.take());
    assertEquals(r6, q.take());
    assertEquals(w2, q.take());
  }

  /** With writeQuota=2, two writes are drained after each read batch. */
  @Test
  public void testWriteQuotaAfterReadBatch() throws InterruptedException {
    Configuration conf = new Configuration();
    conf.setInt("ns." + ReadWriteCallQueue.READ_BATCH_KEY, 2);
    conf.setInt("ns." + ReadWriteCallQueue.WRITE_QUOTA_KEY, 2);
    ReadWriteCallQueue<Schedulable> q =
        new ReadWriteCallQueue<Schedulable>(1, 100, "ns", conf);

    Schedulable r1 = mockCall("r1", false);
    Schedulable r2 = mockCall("r2", false);
    Schedulable w1 = mockCall("w1", true);
    Schedulable w2 = mockCall("w2", true);
    Schedulable w3 = mockCall("w3", true);
    q.put(r1);
    q.put(r2);
    q.put(w1);
    q.put(w2);
    q.put(w3);

    assertEquals(r1, q.take());
    assertEquals(r2, q.take());
    assertEquals(w1, q.take());
    assertEquals(w2, q.take());
    assertEquals(w3, q.take());
  }

  /** When one side is empty, the other side is served immediately. */
  @Test
  public void testEmptyWriteFallsBackToRead() throws InterruptedException {
    ReadWriteCallQueue<Schedulable> q =
        new ReadWriteCallQueue<Schedulable>(1, 100, "ns", new Configuration());
    Schedulable r1 = mockCall("r1", false);
    Schedulable r2 = mockCall("r2", false);
    q.put(r1);
    q.put(r2);
    assertEquals(r1, q.take());
    assertEquals(r2, q.take());
  }

  @Test
  public void testEmptyReadFallsBackToWrite() throws InterruptedException {
    ReadWriteCallQueue<Schedulable> q =
        new ReadWriteCallQueue<Schedulable>(1, 100, "ns", new Configuration());
    Schedulable w1 = mockCall("w1", true);
    Schedulable w2 = mockCall("w2", true);
    q.put(w1);
    q.put(w2);
    assertEquals(w1, q.take());
    assertEquals(w2, q.take());
  }

  @Test
  public void testTakeRemovesCall() throws InterruptedException {
    Schedulable call = mockCall("c", true);
    rwq.offer(call);
    assertEquals(call, rwq.take());
    assertEquals(0, rwq.size());
  }

  @Test
  public void testTakeBlocksWhenEmpty() throws InterruptedException {
    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(1);
    Thread t =
        new Thread(
            () -> {
              try {
                started.countDown();
                rwq.take(); // blocks until a put below
                done.countDown();
              } catch (InterruptedException ignored) {
              }
            });
    t.start();
    started.await();
    rwq.put(mockCall("c", true));
    assertTrue(done.await(5, TimeUnit.SECONDS));
  }

  @Test
  public void testPutBlocksWhenFullThenWakes() throws InterruptedException {
    for (int i = 0; i < 10; i++) {
      assertTrue(rwq.offer(mockCall("c" + i)));
    }
    assertEquals(10, rwq.size());

    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(1);
    Thread t =
        new Thread(
            () -> {
              try {
                started.countDown();
                rwq.put(mockCall("blocked"));
                // blocks until a take frees capacity
                done.countDown();
              } catch (InterruptedException ignored) {
              }
            });
    t.start();
    started.await();
    // give the putter a moment to block
    Thread.sleep(200);
    rwq.take(); // free one slot -> putter should proceed
    assertTrue(done.await(5, TimeUnit.SECONDS));
    assertEquals(10, rwq.size());
  }

  // ------------------- bulk ops -------------------

  @Test
  public void testContains() {
    Schedulable call = mockCall("c", true);
    rwq.offer(call);
    assertTrue(rwq.contains(call));
    assertFalse(rwq.contains(mockCall("other", true)));
  }

  @Test
  public void testDrainTo() {
    ReadWriteCallQueue<Schedulable> src =
        new ReadWriteCallQueue<Schedulable>(1, 10, "ns", new Configuration());
    ReadWriteCallQueue<Schedulable> dst =
        new ReadWriteCallQueue<Schedulable>(1, 10, "ns", new Configuration());
    src.offer(mockCall("w", true));
    src.offer(mockCall("r", false));
    src.offer(mockCall("w2", true));

    src.drainTo(dst);
    assertEquals(0, src.size());
    assertEquals(3, dst.size());
  }

  @Test
  public void testDrainToWithLimit() {
    ReadWriteCallQueue<Schedulable> src =
        new ReadWriteCallQueue<Schedulable>(1, 10, "ns", new Configuration());
    ReadWriteCallQueue<Schedulable> dst =
        new ReadWriteCallQueue<Schedulable>(1, 10, "ns", new Configuration());
    src.offer(mockCall("w", true));
    src.offer(mockCall("r", false));
    src.offer(mockCall("w2", true));

    src.drainTo(dst, 2);
    assertEquals(1, src.size());
    assertEquals(2, dst.size());
  }

  // ------------------- metrics -------------------

  @Test
  public void testMetricsQueueSizes() throws Exception {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName mxbeanName = new ObjectName(
        "Hadoop:service=ns,name=ReadWriteCallQueue");

    Schedulable w = mockCall("w", true);
    Schedulable r = mockCall("r", false);
    rwq.offer(w);
    rwq.offer(r);

    assertEquals(1, mbs.getAttribute(mxbeanName, "ReadQueueSize"));
    assertEquals(1, mbs.getAttribute(mxbeanName, "WriteQueueSize"));

    // Scheduler starts in read phase, so the read is taken first; when the
    // read sub-queue empties it falls back to the waiting write.
    assertEquals(r, rwq.take());
    assertEquals(0, mbs.getAttribute(mxbeanName, "ReadQueueSize"));
    assertEquals(1, mbs.getAttribute(mxbeanName, "WriteQueueSize"));
    assertEquals(w, rwq.take());
    assertEquals(0, mbs.getAttribute(mxbeanName, "WriteQueueSize"));
  }

  @Test
  public void testMetricsPhaseCounts() throws Exception {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName mxbeanName = new ObjectName(
        "Hadoop:service=ns,name=ReadWriteCallQueue");

    Configuration conf = new Configuration();
    conf.setInt("ns." + ReadWriteCallQueue.READ_BATCH_KEY, 2);
    conf.setInt("ns." + ReadWriteCallQueue.WRITE_QUOTA_KEY, 1);
    ReadWriteCallQueue<Schedulable> q =
        new ReadWriteCallQueue<Schedulable>(1, 100, "ns", conf);

    q.put(mockCall("r1", false));
    q.put(mockCall("r2", false));
    q.put(mockCall("w1", true));
    q.put(mockCall("r3", false));

    long readsBefore = (Long) mbs.getAttribute(mxbeanName, "ReadPhaseCount");
    long writesBefore = (Long) mbs.getAttribute(mxbeanName, "WritePhaseCount");

    q.take(); // r1
    q.take(); // r2
    q.take(); // w1 -> switches to write phase

    long readsAfter = (Long) mbs.getAttribute(mxbeanName, "ReadPhaseCount");
    long writesAfter = (Long) mbs.getAttribute(mxbeanName, "WritePhaseCount");

    assertEquals(readsBefore + 1, readsAfter);
    assertEquals(writesBefore + 1, writesAfter);
  }

  // ------------------- FairCallQueue alignment / honest failure ----------

  /**
   * Like FairCallQueue, iteration is unsupported and must fail loudly rather
   * than silently corrupting the two sub-queues.
   */
  @Test(expected = NotImplementedException.class)
  public void testIteratorThrows() {
    rwq.offer(mockCall("r", false));
    rwq.iterator();
  }

  /** remove(Object) relies on iterator(), so it must also fail loudly. */
  @Test(expected = NotImplementedException.class)
  public void testRemoveObjectThrows() {
    rwq.offer(mockCall("r", false));
    rwq.remove(mockCall("other", false));
  }

  @Test(expected = NotImplementedException.class)
  public void testRemoveAllThrows() {
    rwq.offer(mockCall("r", false));
    rwq.removeAll(java.util.Collections.<Schedulable>singletonList(
        mockCall("x", false)));
  }

  @Test(expected = NotImplementedException.class)
  public void testRetainAllThrows() {
    rwq.offer(mockCall("r", false));
    rwq.retainAll(java.util.Collections.<Schedulable>singletonList(
        mockCall("x", false)));
  }

  // ------------------- capacity invariants / contract -------------------

  /** size() + remainingCapacity() must always equal the configured capacity. */
  @Test
  public void testSizeRemainingCapacityInvariant() {
    assertEquals(10, rwq.size() + rwq.remainingCapacity());
    for (int i = 0; i < 4; i++) {
      rwq.offer(mockCall("c" + i));
    }
    assertEquals(4, rwq.size());
    assertEquals(6, rwq.remainingCapacity());
    assertEquals(10, rwq.size() + rwq.remainingCapacity());
  }

  /** BlockingQueue contract: offering null must throw NPE. */
  @Test(expected = NullPointerException.class)
  public void testOfferNullThrowsNPE() {
    rwq.offer(null);
  }

  /**
   * add() on a full queue must throw IllegalStateException (not return false).
   */
  @Test
  public void testAddWhenFullThrows() {
    for (int i = 0; i < 10; i++) {
      assertTrue(rwq.offer(mockCall("c" + i)));
    }
    try {
      rwq.add(mockCall("extra"));
      fail("add() on a full queue should have thrown");
    } catch (IllegalStateException expected) {
      // expected
    }
    assertEquals(10, rwq.size());
  }

  // ------------------- drainTo edge cases -------------------

  @Test(expected = IllegalArgumentException.class)
  public void testDrainToSelfThrows() {
    rwq.offer(mockCall("r", false));
    rwq.drainTo(rwq);
  }

  @Test
  public void testDrainToWithZeroLimit() {
    ReadWriteCallQueue<Schedulable> src =
        new ReadWriteCallQueue<Schedulable>(1, 10, "ns", new Configuration());
    ReadWriteCallQueue<Schedulable> dst =
        new ReadWriteCallQueue<Schedulable>(1, 10, "ns", new Configuration());
    src.offer(mockCall("w", true));
    src.offer(mockCall("r", false));
    assertEquals(0, src.drainTo(dst, 0));
    assertEquals(2, src.size());
    assertEquals(0, dst.size());
  }

  @Test
  public void testDrainToEmpty() {
    ReadWriteCallQueue<Schedulable> src =
        new ReadWriteCallQueue<Schedulable>(1, 10, "ns", new Configuration());
    ReadWriteCallQueue<Schedulable> dst =
        new ReadWriteCallQueue<Schedulable>(1, 10, "ns", new Configuration());
    assertEquals(0, src.drainTo(dst));
    assertEquals(0, src.size());
    assertEquals(0, dst.size());
  }

  /** A limit larger than the queue size just drains everything. */
  @Test
  public void testDrainToLimitLargerThanSize() {
    ReadWriteCallQueue<Schedulable> src =
        new ReadWriteCallQueue<Schedulable>(1, 10, "ns", new Configuration());
    ReadWriteCallQueue<Schedulable> dst =
        new ReadWriteCallQueue<Schedulable>(1, 10, "ns", new Configuration());
    src.offer(mockCall("w", true));
    src.offer(mockCall("r", false));
    src.offer(mockCall("w2", true));
    assertEquals(3, src.drainTo(dst, 100));
    assertEquals(0, src.size());
    assertEquals(3, dst.size());
  }

  // ------------------- peek / element / remove semantics -------------------

  /** peek() is best-effort and returns the write head first by design. */
  @Test
  public void testPeekPrefersWriteHead() throws InterruptedException {
    Schedulable r = mockCall("r", false);
    Schedulable w = mockCall("w", true);
    rwq.offer(r);
    rwq.offer(w);
    assertEquals(w, rwq.peek());
    assertEquals(2, rwq.size()); // non-destructive
    // The scheduler still serves the read first.
    assertEquals(r, rwq.take());
  }

  @Test
  public void testElementReturnsPeek() {
    Schedulable r = mockCall("r", false);
    rwq.offer(r);
    assertEquals(r, rwq.element());
    assertEquals(r, rwq.peek());
  }

  /** Queue.remove() delegates to poll(), honoring the read-first schedule. */
  @Test
  public void testRemoveReturnsScheduledHead() {
    Schedulable r = mockCall("r", false);
    Schedulable w = mockCall("w", true);
    rwq.offer(r);
    rwq.offer(w);
    assertEquals(r, rwq.remove());
    assertEquals(1, rwq.size());
  }

  // ------------------- scheduling boundary cases -------------------

  /** Minimal batch (readBatch=1, writeQuota=1) enforces strict alternation. */
  @Test
  public void testMinimalBatchAlternation() throws InterruptedException {
    Configuration conf = new Configuration();
    conf.setInt("ns." + ReadWriteCallQueue.READ_BATCH_KEY, 1);
    conf.setInt("ns." + ReadWriteCallQueue.WRITE_QUOTA_KEY, 1);
    ReadWriteCallQueue<Schedulable> q =
        new ReadWriteCallQueue<Schedulable>(1, 100, "ns", conf);

    Schedulable r1 = mockCall("r1", false);
    Schedulable w1 = mockCall("w1", true);
    Schedulable r2 = mockCall("r2", false);
    Schedulable w2 = mockCall("w2", true);
    q.put(r1);
    q.put(w1);
    q.put(r2);
    q.put(w2);

    assertEquals(r1, q.take());
    assertEquals(w1, q.take());
    assertEquals(r2, q.take());
    assertEquals(w2, q.take());
  }

  /**
   * readBatch=5 but only two reads are present: once the read sub-queue
   * empties, the write is served immediately instead of waiting for 5 reads.
   */
  @Test
  public void testReadsExhaustedBeforeBatchCompletes()
      throws InterruptedException {
    Configuration conf = new Configuration();
    conf.setInt("ns." + ReadWriteCallQueue.READ_BATCH_KEY, 5);
    conf.setInt("ns." + ReadWriteCallQueue.WRITE_QUOTA_KEY, 1);
    ReadWriteCallQueue<Schedulable> q =
        new ReadWriteCallQueue<Schedulable>(1, 100, "ns", conf);

    Schedulable r1 = mockCall("r1", false);
    Schedulable r2 = mockCall("r2", false);
    Schedulable w1 = mockCall("w1", true);
    q.put(r1);
    q.put(r2);
    q.put(w1);

    assertEquals(r1, q.take());
    assertEquals(r2, q.take());
    // Only two reads were present (well below readBatch=5); the write is served
    // immediately the read sub-queue empties rather than waiting for 5 reads.
    assertEquals(w1, q.take());
  }

  /** Symmetric to the above: a short write batch falls back to reads. */
  @Test
  public void testWriteQuotaExhaustedMidBatchFallsToRead()
      throws InterruptedException {
    Configuration conf = new Configuration();
    conf.setInt("ns." + ReadWriteCallQueue.READ_BATCH_KEY, 2);
    conf.setInt("ns." + ReadWriteCallQueue.WRITE_QUOTA_KEY, 3);
    ReadWriteCallQueue<Schedulable> q =
        new ReadWriteCallQueue<Schedulable>(1, 100, "ns", conf);

    Schedulable r1 = mockCall("r1", false);
    Schedulable r2 = mockCall("r2", false);
    Schedulable w1 = mockCall("w1", true);
    Schedulable r3 = mockCall("r3", false);
    q.put(r1);
    q.put(r2);
    q.put(w1);
    q.put(r3);

    assertEquals(r1, q.take());
    assertEquals(r2, q.take());
    assertEquals(w1, q.take());
    assertEquals(r3, q.take());
  }

  // ------------------- metrics -------------------

  /**
   * A freshly constructed queue starts in the read phase, so the initial read
   * phase must be counted (readPhaseCount == 1) while no write phase has
   * occurred yet (writePhaseCount == 0).
   */
  @Test
  public void testMetricsInitialPhaseCounts() throws Exception {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName mxbeanName = new ObjectName(
        "Hadoop:service=ns,name=ReadWriteCallQueue");
    assertEquals(1L, mbs.getAttribute(mxbeanName, "ReadPhaseCount"));
    assertEquals(0L, mbs.getAttribute(mxbeanName, "WritePhaseCount"));
  }

  // ------------------- concurrency -------------------

  /**
   * Many producers and consumers racing on the queue must not lose, duplicate,
   * or corrupt any element. Capacity is small so producers contend on offer().
   */
  @Test
  public void testConcurrentOfferTakeNoLoss() throws Exception {
    final ReadWriteCallQueue<Schedulable> q =
        new ReadWriteCallQueue<Schedulable>(1, 100, "nsc", new Configuration());
    final int numProducers = 4;
    final int perProducer = 500;
    final int total = numProducers * perProducer;

    final Set<Schedulable> seen = ConcurrentHashMap.newKeySet();
    final AtomicInteger taken = new AtomicInteger(0);
    final AtomicBoolean producersDone = new AtomicBoolean(false);
    final AtomicBoolean duplicate = new AtomicBoolean(false);

    Thread[] producers = new Thread[numProducers];
    for (int p = 0; p < numProducers; p++) {
      final int pid = p;
      producers[p] = new Thread(() -> {
        for (int i = 0; i < perProducer; i++) {
          Schedulable c = mockCall(pid + "-" + i, (i % 2 == 0));
          while (!q.offer(c)) {
            Thread.yield();
          }
        }
      });
    }

    int numConsumers = 4;
    Thread[] consumers = new Thread[numConsumers];
    for (int c = 0; c < numConsumers; c++) {
      consumers[c] = new Thread(() -> {
        try {
          while (taken.get() < total) {
            Schedulable e = q.poll(50, TimeUnit.MILLISECONDS);
            if (e != null) {
              if (!seen.add(e)) {
                duplicate.set(true);
                break;
              }
              taken.incrementAndGet();
            } else if (producersDone.get() && taken.get() < total) {
              break;
            }
          }
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      });
    }

    for (Thread t : producers) {
      t.start();
    }
    for (Thread t : consumers) {
      t.start();
    }
    for (Thread t : producers) {
      t.join();
    }
    producersDone.set(true);
    for (Thread t : consumers) {
      t.join(10000);
    }

    assertFalse("queue delivered a duplicate element", duplicate.get());
    assertEquals("every element taken exactly once", total, taken.get());
    assertEquals("unique elements match produced count", total, seen.size());
    assertEquals(0, q.size());
  }

  // ------------------- interruption -------------------

  /** put() on a full queue must be interruptible. */
  @Test
  public void testPutInterruptible() throws Exception {
    for (int i = 0; i < 10; i++) {
      rwq.offer(mockCall("c" + i));
    }
    final AtomicBoolean gotInterrupt = new AtomicBoolean(false);
    Thread t = new Thread(() -> {
      try {
        rwq.put(mockCall("blocked")); // blocks: queue is full
      } catch (InterruptedException e) {
        gotInterrupt.set(true);
      }
    });
    t.start();
    Thread.sleep(100);
    t.interrupt();
    t.join(5000);
    assertTrue(gotInterrupt.get());
    assertEquals(10, rwq.size()); // the blocked put never completed
  }

  /** take() on an empty queue must be interruptible. */
  @Test
  public void testTakeInterruptible() throws Exception {
    final AtomicBoolean gotInterrupt = new AtomicBoolean(false);
    Thread t = new Thread(() -> {
      try {
        rwq.take(); // blocks: queue is empty
      } catch (InterruptedException e) {
        gotInterrupt.set(true);
      }
    });
    t.start();
    Thread.sleep(100);
    t.interrupt();
    t.join(5000);
    assertTrue(gotInterrupt.get());
  }

}
