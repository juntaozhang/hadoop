/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license
 * agreements. See the NOTICE file distributed with this work for additional
 * information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache
 * License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
 * OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.ipc.ReadWriteCallQueue;
import org.apache.hadoop.ipc.Schedulable;
import org.apache.hadoop.metrics2.lib.MutableRatesWithAggregation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Micro-benchmark that compares two NameNode call-queue dequeue policies under
 * the real {@code
 * FSNamesystemLock} bottleneck.
 *
 * <ul>
 * <li>{@code fifo} — handlers take calls in pure arrival order (plain queue);
 *   <li>{@code readwrite} — handlers take calls from the {
 *
 * @code ReadWriteCallQueue}, which batches reads together so they can hold the
 * lock concurrently.
 *     </ul>
 * <p>For each configuration the same deterministic arrival sequence is
 * replayed by both modes
 * in the same JVM, so the reported {@code gain} reflects the queue policy
 * only. {@code --first}
 * controls which mode runs first (to factor out JVM warmup), and {@code
 * --fair} toggles the
 * lock's fairness ({@code false} lets readers barge in for higher read
 * throughput, {@code true}
 *     gives queued writers priority).
 *     <p>Help:
 *     <pre>
 *   single run : --ratio 1:10 --mode readwrite --handlers 64
 *   scan many  : --ratio 1:6,1:10 --handlers 32,64 --fair true,false
 *   producers  : --producers 4   (raise if handlers gate waiting for take())
 *   rpc mix    : --mix "getFileInfo:R:30:10,listStatus:R:5:200,create:W:8:60"
 *                (each entry: name:R|W:weight:holdMicros)
 *   scheduler  : --readBatch 32 --writeBatch 8   (readwrite batch sizes)
 *   by count   : --warmupOps 20000 --ops 200000
 *   order      : --first readwrite
 *   </pre>
 *   <p>Examples:
 *   <pre>
 *   --mix getFileInfo:R:30:10,listStatus:R:5:200,create:W:8:60,addBlock:W:2:150
 *   --ratio 1:6
 *   --fair false
 *   --handlers 128
 *   --producers 4
 *   --readBatch 128
 *   --writeBatch 32
 *   --runs 1
 *   --warmupOps 20000
 *   --ops 200000
 * </pre>
 * <p>{@code --ratio}, {@code --handlers}, {@code --fair} and {@code --mode}
 * all accept
 *     comma-separated lists and every combination is benchmarked.
 */
public class ReadWriteCallQueueBenchmark implements Tool {
  /** Length of the busy-spun tail of a lock hold. */
  private static final long SPIN_TAIL_NANOS = TimeUnit.MICROSECONDS.toNanos(50);
  private final Log logger;
  private Configuration conf;
  private final UserGroupInformation ugi;
  public ReadWriteCallQueueBenchmark() {
    if (System.getProperty("hadoop.log.dir") == null) {
      System.setProperty("hadoop.log.dir", "/tmp");
    }
    logger = LogFactory.getLog(ReadWriteCallQueueBenchmark.class);
    ugi = UserGroupInformation.createRemoteUser("benchmark");
    logger.info("Start benchmark..");
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * A synthetic RPC "profile": a name, whether it takes the write lock, a
   * relative weight (how
   * often it shows up in the workload), and a base lock-hold duration.
   *
   * <p>A real NameNode sees many different RPCs with very different hold costs
   * (a cheap {@code
   * getFileInfo} vs a {@code listStatus} on a huge directory vs an {@code
   * addBlock} that syncs the
   * edit log). Modelling the workload as a weighted mix of several profiles
   * produces a realistic,
   * multi-modal hold-time distribution instead of a single read hold and a
   * single write hold.
   */
  private static final class RpcProfile {
    private final String name;
    private final boolean write;
    private final double weight;
    private final long baseHoldNanos;

    RpcProfile(String name, boolean write, double weight, long baseHoldNanos) {
      this.name = name;
      this.write = write;
      this.weight = weight;
      this.baseHoldNanos = baseHoldNanos;
    }
  }

  /**
   * A synthetic RPC call; implements Schedulable so it can live in
   * ReadWriteCallQueue.
   */
  private final class Call implements Schedulable {
    private final RpcProfile profile;

    Call(RpcProfile profile) {
      this.profile = profile;
    }

    @Override
    public boolean isWrite() {
      return profile.write;
    }

    @Override
    public int getPriorityLevel() {
      return 0;
    }

    @Override
    public UserGroupInformation getUserGroupInformation() {
      return ugi;
    }
  }

  /**
   * Per-type (read vs write) latency recorder: lock wait and lock hold, in
   * nanos.
   */
  private static final class Recorder {
    private final List<List<Long>> waitSamples =
        Arrays.asList(new ArrayList<Long>(), new ArrayList<Long>());
    private final List<List<Long>> holdSamples =
        Arrays.asList(new ArrayList<Long>(), new ArrayList<Long>());

    void record(boolean isWrite, long waitNanos, long holdNanos) {
      synchronized (this) {
        waitSamples.get(isWrite ? 1 : 0).add(waitNanos);
        holdSamples.get(isWrite ? 1 : 0).add(holdNanos);
      }
    }

    synchronized Stats stats(boolean isWrite) {
      return new Stats(waitSamples.get(isWrite ? 1 : 0),
          holdSamples.get(isWrite ? 1 : 0));
    }
  }

  private static final class Stats {
    private final long count;
    private final double waitAvgUs;
    private final long waitP50Us, waitP99Us, waitP999Us;
    private final double holdAvgUs;

    Stats(List<Long> waits, List<Long> holds) {
      this.count = waits.size();
      this.waitAvgUs = avgUs(waits);
      this.waitP50Us = pctUs(waits, 50);
      this.waitP99Us = pctUs(waits, 99);
      this.waitP999Us = pctUs(waits, 99.9);
      this.holdAvgUs = avgUs(holds);
    }

    private static double avgUs(List<Long> nanos) {
      if (nanos.isEmpty()) {
        return 0;
      }
      double sum = 0;
      for (long n : nanos) {
        sum += n;
      }
      return sum / nanos.size() / 1000.0;
    }

    private static long pctUs(List<Long> nanos, double pct) {
      if (nanos.isEmpty()) {
        return 0;
      }
      List<Long> sorted = new ArrayList<Long>(nanos);
      Collections.sort(sorted);
      int idx = (int) Math.min(sorted.size() - 1, Math.ceil(sorted.size() * pct
          / 100.0) - 1);
      return TimeUnit.NANOSECONDS.toMicros(sorted.get(Math.max(0, idx)));
    }
  }

  /** A run of the benchmark with one configuration. */
  private final class Benchmark {
    private final String mode; // fifo | readwrite
    private final int writeNumerator;
    private final int readNumerator;
    private final boolean fair;
    private final int handlers;
    private final int producers;
    private final int capacity;
    // count mode: #calls discarded as warmup before measuring; 0 => none
    private final long warmupOps;
    private final int readBatch;
    // max reads released per batch (readwrite mode)
    private final int writeBatch;
    // max writes released per batch (readwrite mode)
    private final double holdJitter; // 0..1 uniform spread around base hold
    private final double holdSpikeProb;
    // fraction of ops that are latency spikes
    private final double holdSpikeMult; // spike multiplier over base hold
    private final List<RpcProfile> profiles;
    // the RPC mix (defaults to READ/WRITE)
    private final List<Call> warmupSequence;
    // count-mode warmup prefix (discarded); may be empty
    private final List<Call> measuredSequence;
    // the measured batch (exact per-type composition)
    private final long stopAfter;
    // count mode: exact #measured calls to consume; <=0 => only safety cap

    /**
     * Canonical ctor: takes the already-built (and possibly shared) batches.
     */
    Benchmark(
        Map<String, String> args,
        List<RpcProfile> profiles,
        List<Call> warmupSequence,
        List<Call> measuredSequence,
        long stopAfter) {
      this.mode = args.getOrDefault("mode", "fifo");
      String[] ratio = args.getOrDefault("ratio", "1:10").split(":");
      this.writeNumerator = Integer.parseInt(ratio[0].trim());
      this.readNumerator = Integer.parseInt(ratio[1].trim());
      this.fair = Boolean.parseBoolean(args.getOrDefault("fair", "true"));
      this.handlers = Integer.parseInt(args.getOrDefault("handlers", "64"));
      this.producers = Integer.parseInt(args.getOrDefault("producers", "1"));
      this.capacity = Integer.parseInt(args.getOrDefault("capacity", "10000"));
      this.warmupOps = Long.parseLong(args.getOrDefault("warmupOps", "0"));
      this.readBatch = Integer.parseInt(args.getOrDefault("readBatch", "32"));
      // writes served per cycle; default keeps pace with the read:write
      // ratio so the write lane cannot grow without bound
      int parsedWriteQuota = Integer.parseInt(args.getOrDefault("writeBatch",
          "-1"));
      this.writeBatch =
          parsedWriteQuota > 0
              ? parsedWriteQuota
              : Math.max(1, Math.round(readBatch * (float) writeNumerator /
                  readNumerator));
      this.holdJitter = Double.parseDouble(args.getOrDefault("holdJitter",
          "0.5"));
      this.holdSpikeProb = Double.parseDouble(args.getOrDefault("holdSpikeProb",
          "0.02"));
      this.holdSpikeMult = Double.parseDouble(args.getOrDefault("holdSpikeMult",
          "10"));
      this.profiles = profiles;
      this.warmupSequence = warmupSequence;
      this.measuredSequence = measuredSequence;
      this.stopAfter = stopAfter;
    }

    String header() {
      return "mode,ratio,fair,handlers,producers,capacity,readBatch,writeBatch,"
          + "opsPerSec,readOpsPerSec,writeOpsPerSec,"
          + "readWaitAvgUs,readWaitP50Us,readWaitP99Us,readWaitP999Us,"
          + "writeWaitAvgUs,writeWaitP50Us,writeWaitP99Us,writeWaitP999Us,"
          + "readHoldAvgUs,writeHoldAvgUs,lockQueueAvg,lockQueueMax,"
          + "concurrentReadersAvg,concurrentReadersMax,"
          + "writeLockBusyPct,readRuns,writeAcquires,avgReadsPerRun,"
          + "readQueueSize,writeQueueSize,readPhaseCount,writePhaseCount";
    }

    // Per-run shared state, populated at the start of runOnce(). These are
    // instance fields (not locals) so the filler/sampler/handler threads can
    // reach them without exceeding the ParameterNumber limit.
    private AtomicLong consumed;
    private AtomicBoolean recording;
    private AtomicBoolean running;
    private AtomicLong measureStart;
    private ConcurrentHashMap<String, AtomicLong> rpcCount;
    private ConcurrentHashMap<String, AtomicLong> rpcHoldSum;
    private Metrics metrics;
    private CountDownLatch done;

    /** Lock + reader/writer tally shared by the sampler and handler threads. */
    private final class Metrics {
      private final AtomicLong lockQueueSum;
      private final AtomicLong lockQueueMax;
      private final AtomicLong lockQueueSamples;
      private final AtomicLong concurrentReadersSum;
      private final AtomicLong concurrentReadersMax;
      private final AtomicLong readRuns;
      private final AtomicLong writeAcquires;

      Metrics(
          AtomicLong lockQueueSum,
          AtomicLong lockQueueMax,
          AtomicLong lockQueueSamples,
          AtomicLong concurrentReadersSum,
          AtomicLong concurrentReadersMax,
          AtomicLong readRuns,
          AtomicLong writeAcquires) {
        this.lockQueueSum = lockQueueSum;
        this.lockQueueMax = lockQueueMax;
        this.lockQueueSamples = lockQueueSamples;
        this.concurrentReadersSum = concurrentReadersSum;
        this.concurrentReadersMax = concurrentReadersMax;
        this.readRuns = readRuns;
        this.writeAcquires = writeAcquires;
      }
    }

    Result runOnce() throws Exception {
      conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_FSLOCK_FAIR_KEY, fair);
      conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_LOCK_DETAILED_METRICS_KEY,
          false);
      final FSNamesystemLock fsLock =
          new FSNamesystemLock(conf, new MutableRatesWithAggregation());

      // The warmup + measured batches are built once in the constructor
      // (this.warmupSequence / this.measuredSequence / this.stopAfter) so
      // every
      // runOnce() replays the identical deterministic batches.
      final Recorder recorder = new Recorder();
      rpcCount = new ConcurrentHashMap<String, AtomicLong>();
      rpcHoldSum = new ConcurrentHashMap<String, AtomicLong>();
      for (RpcProfile p : profiles) {
        rpcCount.put(p.name, new AtomicLong());
        rpcHoldSum.put(p.name, new AtomicLong());
      }
      consumed = new AtomicLong();
      measureStart = new AtomicLong();
      running = new AtomicBoolean(true);
      recording = new AtomicBoolean(false);
      metrics =
          new Metrics(
              new AtomicLong(),
              new AtomicLong(),
              new AtomicLong(),
              new AtomicLong(),
              new AtomicLong(),
              new AtomicLong(),
              new AtomicLong());

      // Dequeue structure differs per mode.
      final BlockingQueue<Call> fifoQueue =
          "fifo".equals(mode) ? new ArrayBlockingQueue<Call>(capacity) : null;
      getConf().setInt("ns.readwrite.read.batch", readBatch);
      getConf().setInt("ns.readwrite.write.batch", writeBatch);
      final ReadWriteCallQueue<Call> rwQueue =
          "readwrite".equals(mode)
              ? new ReadWriteCallQueue<>(1, capacity, "ns", getConf())
              : null;
      final BlockingQueue<Call> source = "fifo".equals(mode) ? fifoQueue :
          rwQueue;

      List<Thread> fillers = buildFillers(source);
      Thread sampler = buildSampler(fsLock);
      List<Thread> handlerThreads = buildHandlers(fsLock, source, recorder);
      for (Thread f : fillers) {
        f.start();
      }
      sampler.start();
      for (Thread t : handlerThreads) {
        t.start();
      }

      // Count mode only: run until the configured number of ops is consumed
      // (or the safety deadline elapses), with a 600s cap to avoid hangs.
      recording.set(warmupOps == 0);
      long start = System.nanoTime();
      long deadline = start + TimeUnit.SECONDS.toNanos(600);
      while (running.get() && System.nanoTime() < deadline) {
        Thread.sleep(50);
      }
      long elapsedNanos =
          (warmupOps > 0 && measureStart.get() > 0)
              ? (System.nanoTime() - measureStart.get())
              : (System.nanoTime() - start);
      recording.set(false);
      running.set(false);
      for (Thread t : handlerThreads) {
        t.interrupt();
      }
      for (Thread f : fillers) {
        f.interrupt();
      }
      sampler.interrupt();
      done.await(5, TimeUnit.SECONDS);

      Stats readStats = recorder.stats(false);
      Stats writeStats = recorder.stats(true);
      long totalOps = readStats.count + writeStats.count;
      Map<String, double[]> rpcHold = new LinkedHashMap<String, double[]>();
      for (RpcProfile p : profiles) {
        long c = rpcCount.get(p.name).get();
        long sum = rpcHoldSum.get(p.name).get();
        rpcHold.put(p.name, new double[] {c, c == 0 ? 0 : sum / (double) c /
                1000.0});
      }
      double seconds = elapsedNanos / 1e9;
      long samples = Math.max(1, metrics.lockQueueSamples.get());
      long runs = metrics.readRuns.get();
      // Snapshot queue metrics (readwrite only; 0 for fifo).
      long readQueueSize = rwQueue != null ? rwQueue.getReadQueueSize() : 0;
      long writeQueueSize = rwQueue != null ? rwQueue.getWriteQueueSize() : 0;
      long readPhaseCount = rwQueue != null ? rwQueue.getReadPhaseCount() : 0;
      long writePhaseCount = rwQueue != null ? rwQueue.getWritePhaseCount() : 0;
      return new Result(
          new Rates(
              totalOps / seconds,
              readStats.count / seconds,
              writeStats.count / seconds),
          readStats,
          writeStats,
          new LockMetrics(
              (double) metrics.lockQueueSum.get() / samples,
              metrics.lockQueueMax.get(),
              (double) metrics.concurrentReadersSum.get() / samples,
              metrics.concurrentReadersMax.get(),
              100.0 * writeStats.count * writeStats.holdAvgUs / (seconds *
                  1e6)),
          new RunMetrics(
              runs,
              metrics.writeAcquires.get(),
              runs == 0 ? 0 : (double) readStats.count / runs,
              readQueueSize,
              writeQueueSize,
              readPhaseCount,
              writePhaseCount),
          rpcHold);
    }

    private List<Thread> buildFillers(BlockingQueue<Call> source) {
      List<Thread> fillers = new ArrayList<Thread>();
      for (int p = 0; p < producers; p++) {
        Thread filler = new Thread(new Filler(source, p), "filler-" + p);
        filler.setDaemon(true);
        fillers.add(filler);
      }
      return fillers;
    }

    private Thread buildSampler(FSNamesystemLock fsLock) {
      Thread sampler = new Thread(new Sampler(fsLock), "queue-sampler");
      sampler.setDaemon(true);
      return sampler;
    }

    private List<Thread> buildHandlers(
        FSNamesystemLock fsLock,
        BlockingQueue<Call> source,
        Recorder recorder) {
      done = new CountDownLatch(handlers);
      List<Thread> handlerThreads = new ArrayList<Thread>();
      for (int i = 0; i < handlers; i++) {
        Thread t = new Thread(new Handler(fsLock, source, recorder),
            "handler-" + i);
        t.setDaemon(true);
        handlerThreads.add(t);
      }
      return handlerThreads;
    }

    /**
     * Producer thread: pushes the warmup prefix and the measured batch into the
     * queue. The warmup prefix is drained (and discarded by handlers) before
     * the measured batch is released so the recorded window has an exact
     * per-type composition.
     */
    private final class Filler implements Runnable {
      private final BlockingQueue<Call> source;
      private final int startIdx;

      Filler(BlockingQueue<Call> source, int startIdx) {
        this.source = source;
        this.startIdx = startIdx;
      }

      @Override
      public void run() {
        for (int i = startIdx; i < warmupSequence.size(); i += producers) {
          try {
            source.put(warmupSequence.get(i));
          } catch (InterruptedException e) {
            return;
          }
        }
        while (running.get() && consumed.get() < warmupOps) {
          try {
            Thread.sleep(1);
          } catch (InterruptedException e) {
            return;
          }
        }
        for (int i = startIdx; i < measuredSequence.size(); i += producers) {
          try {
            source.put(measuredSequence.get(i));
          } catch (InterruptedException e) {
            return;
          }
        }
      }
    }

    /** Sampler thread: polls lock-queue length and reader concurrency. */
    private final class Sampler implements Runnable {
      private final FSNamesystemLock fsLock;

      Sampler(FSNamesystemLock fsLock) {
        this.fsLock = fsLock;
      }

      @Override
      public void run() {
        while (running.get()) {
          int len = fsLock.getQueueLength();
          metrics.lockQueueSum.addAndGet(len);
          long max;
          do {
            max = metrics.lockQueueMax.get();
            if (len <= max) {
              break;
            }
          } while (!metrics.lockQueueMax.compareAndSet(max, len));
          int readers = fsLock.coarseLock.getReadLockCount();
          metrics.concurrentReadersSum.addAndGet(readers);
          do {
            max = metrics.concurrentReadersMax.get();
            if (readers <= max) {
              break;
            }
          } while (!metrics.concurrentReadersMax.compareAndSet(max, readers));
          metrics.lockQueueSamples.incrementAndGet();
          try {
            Thread.sleep(1);
          } catch (InterruptedException e) {
            return;
          }
        }
      }
    }

    /**
     * Handler thread: dequeues calls, takes the appropriate lock, holds it for
     * a (noisy) modeled duration, then records wait/hold latencies.
     */
    private final class Handler implements Runnable {
      private final FSNamesystemLock fsLock;
      private final BlockingQueue<Call> source;
      private final Recorder recorder;

      Handler(FSNamesystemLock fsLock, BlockingQueue<Call> source,
          Recorder recorder) {
        this.fsLock = fsLock;
        this.source = source;
        this.recorder = recorder;
      }

      @Override
      public void run() {
        try {
          while (true) {
            Call call = source.take();
            long total = consumed.incrementAndGet();
            boolean rec =
                warmupOps == 0 ? recording.get() : total > warmupOps;
            long t0 = System.nanoTime();
            if (!call.profile.write) {
              fsLock.readLock();
              if (rec && fsLock.coarseLock.getReadLockCount() == 1) {
                metrics.readRuns.incrementAndGet();
              }
            } else {
              fsLock.writeLock();
              if (rec) {
                metrics.writeAcquires.incrementAndGet();
              }
            }
            long t1 = System.nanoTime();
            holdFor(noisyHoldNanos(call.profile.baseHoldNanos));
            long t2 = System.nanoTime();
            if (!call.profile.write) {
              fsLock.readUnlock("benchRead");
            } else {
              fsLock.writeUnlock("benchWrite");
            }
            if (rec) {
              measureStart.compareAndSet(0, t0);
            }
            if (stopAfter > 0 && total >= warmupOps + stopAfter) {
              running.set(false);
            }
            if (rec) {
              recorder.record(call.profile.write, t1 - t0, t2 - t1);
              rpcCount.get(call.profile.name).incrementAndGet();
              rpcHoldSum.get(call.profile.name).addAndGet(t2 - t1);
            }
          }
        } catch (InterruptedException e) {
          // shutdown
        } finally {
          done.countDown();
        }
      }
    }

    private long noisyHoldNanos(long baseNanos) {
      ThreadLocalRandom rnd = ThreadLocalRandom.current();
      double f = 1.0 + (rnd.nextDouble() * 2 - 1) * holdJitter;
      long h = (long) (baseNanos * Math.max(0.1, f));
      if (rnd.nextDouble() < holdSpikeProb) {
        h = (long) (h * holdSpikeMult);
      }
      return h;
    }

    /** Throughput triple, bundled so Result stays within ParameterNumber. */
    private final class Rates {
      private final double opsPerSec;
      private final double readOpsPerSec;
      private final double writeOpsPerSec;

      Rates(double opsPerSec, double readOpsPerSec, double writeOpsPerSec) {
        this.opsPerSec = opsPerSec;
        this.readOpsPerSec = readOpsPerSec;
        this.writeOpsPerSec = writeOpsPerSec;
      }
    }

    /** Lock-queue / reader-concurrency metrics, bundled for ParameterNumber. */
    private final class LockMetrics {
      private final double lockQueueAvg;
      private final double lockQueueMax;
      private final double concurrentReadersAvg;
      private final double concurrentReadersMax;
      private final double writeLockBusyPct;

      LockMetrics(
          double lockQueueAvg,
          double lockQueueMax,
          double concurrentReadersAvg,
          double concurrentReadersMax,
          double writeLockBusyPct) {
        this.lockQueueAvg = lockQueueAvg;
        this.lockQueueMax = lockQueueMax;
        this.concurrentReadersAvg = concurrentReadersAvg;
        this.concurrentReadersMax = concurrentReadersMax;
        this.writeLockBusyPct = writeLockBusyPct;
      }
    }

    /** Per-run counts, bundled so Result stays within ParameterNumber. */
    private final class RunMetrics {
      private final long readRuns;
      private final long writeAcquires;
      private final double avgReadsPerRun;
      private final long readQueueSize;
      private final long writeQueueSize;
      private final long readPhaseCount;
      private final long writePhaseCount;

      RunMetrics(
          long readRuns,
          long writeAcquires,
          double avgReadsPerRun,
          long readQueueSize,
          long writeQueueSize,
          long readPhaseCount,
          long writePhaseCount) {
        this.readRuns = readRuns;
        this.writeAcquires = writeAcquires;
        this.avgReadsPerRun = avgReadsPerRun;
        this.readQueueSize = readQueueSize;
        this.writeQueueSize = writeQueueSize;
        this.readPhaseCount = readPhaseCount;
        this.writePhaseCount = writePhaseCount;
      }
    }

    final class Result {
      private final double opsPerSec, readOpsPerSec, writeOpsPerSec;
      private final Stats read, write;
      private final double lockQueueAvg, lockQueueMax;
      private final double concurrentReadersAvg, concurrentReadersMax;
      private final double writeLockBusyPct;
      private final long readRuns, writeAcquires;
      private final double avgReadsPerRun;
      private final long readQueueSize, writeQueueSize, readPhaseCount,
          writePhaseCount;
      private final Map<String, double[]> rpcHold; // name -> {count, avgHoldUs}

      Result(
          Rates rates,
          Stats read,
          Stats write,
          LockMetrics lm,
          RunMetrics rm,
          Map<String, double[]> rpcHold) {
        this.opsPerSec = rates.opsPerSec;
        this.readOpsPerSec = rates.readOpsPerSec;
        this.writeOpsPerSec = rates.writeOpsPerSec;
        this.read = read;
        this.write = write;
        this.lockQueueAvg = lm.lockQueueAvg;
        this.lockQueueMax = lm.lockQueueMax;
        this.concurrentReadersAvg = lm.concurrentReadersAvg;
        this.concurrentReadersMax = lm.concurrentReadersMax;
        this.writeLockBusyPct = lm.writeLockBusyPct;
        this.readRuns = rm.readRuns;
        this.writeAcquires = rm.writeAcquires;
        this.avgReadsPerRun = rm.avgReadsPerRun;
        this.readQueueSize = rm.readQueueSize;
        this.writeQueueSize = rm.writeQueueSize;
        this.readPhaseCount = rm.readPhaseCount;
        this.writePhaseCount = rm.writePhaseCount;
        this.rpcHold = rpcHold;
      }

      String csv(Benchmark b) {
        return String.format(
            "%s,%d:%d,%b,%d,%d,%d,%d,%d,"
                + "%.0f,%.0f,%.0f,"
                + "%.1f,%d,%d,%d,"
                + "%.1f,%d,%d,%d,"
                + "%.1f,%.1f,%.1f,%d,"
                + "%.2f,%d,"
                + "%.1f,%d,%d,%.2f,"
                + "%d,%d,%d,%d",
            b.mode,
            b.writeNumerator,
            b.readNumerator,
            b.fair,
            b.handlers,
            b.producers,
            b.capacity,
            b.readBatch,
            b.writeBatch,
            opsPerSec,
            readOpsPerSec,
            writeOpsPerSec,
            read.waitAvgUs,
            read.waitP50Us,
            read.waitP99Us,
            read.waitP999Us,
            write.waitAvgUs,
            write.waitP50Us,
            write.waitP99Us,
            write.waitP999Us,
            read.holdAvgUs,
            write.holdAvgUs,
            lockQueueAvg,
            (long) lockQueueMax,
            concurrentReadersAvg,
            (long) concurrentReadersMax,
            writeLockBusyPct,
            readRuns,
            writeAcquires,
            avgReadsPerRun,
            readQueueSize,
            writeQueueSize,
            readPhaseCount,
            writePhaseCount);
      }
    }
  }


  private List<Call> generateSequence(int n, List<RpcProfile>
      profiles) {
    List<Call> seq = new ArrayList<Call>(n);
    Random rand = new Random(42);
    double[] cum = new double[profiles.size()];
    double total = 0;
    for (int i = 0; i < profiles.size(); i++) {
      total += profiles.get(i).weight;
      cum[i] = total;
    }
    for (int i = 0; i < n; i++) {
      double r = rand.nextDouble() * total;
      int idx = Arrays.binarySearch(cum, r);
      if (idx < 0) {
        idx = -idx - 1;
      }
      if (idx >= profiles.size()) {
        idx = profiles.size() - 1;
      }
      seq.add(new Call(profiles.get(idx)));
    }
    return seq;
  }

  /**
   * Count-based composition: the exact number of each RPC type so the total
   * is {@code
   * targetTotal} and every type's count is reproducible (no random boundary
   * overshoot). Rounding
   * drift is absorbed by the heaviest-weighted profile.
   */
  private long[] computeQuotas(long targetTotal, List<RpcProfile>
      profiles) {
    double wSum = 0;
    for (RpcProfile p : profiles) {
      wSum += p.weight;
    }
    long[] quota = new long[profiles.size()];
    long sum = 0;
    for (int i = 0; i < profiles.size(); i++) {
      quota[i] = Math.round(targetTotal * profiles.get(i).weight / wSum);
      if (quota[i] < 0) {
        quota[i] = 0;
      }
      sum += quota[i];
    }
    long diff = targetTotal - sum;
    if (diff != 0) {
      int idx = 0;
      for (int i = 1; i < profiles.size(); i++) {
        if (profiles.get(i).weight > profiles.get(idx).weight) {
          idx = i;
        }
      }
      quota[idx] = Math.max(0, quota[idx] + diff);
    }
    return quota;
  }

  private List<Call> buildExactSequence(long[] quota, List<RpcProfile>
      profiles) {
    List<Call> seq = new ArrayList<Call>();
    for (int i = 0; i < profiles.size(); i++) {
      for (long k = 0; k < quota[i]; k++) {
        seq.add(new Call(profiles.get(i)));
      }
    }
    Collections.shuffle(seq, new Random(42));
    return seq;
  }

  private void holdFor(long nanos) {
    final long deadline = System.nanoTime() + nanos;
    while (true) {
      long remaining = deadline - System.nanoTime();
      if (remaining <= 0) {
        return;
      }
      if (remaining > SPIN_TAIL_NANOS) {
        LockSupport.parkNanos(remaining - SPIN_TAIL_NANOS);
      }
    }
  }

  private static Map<String, String> parseArgs(String[] argv) {
    Map<String, String> args = new LinkedHashMap<String, String>();
    for (int i = 0; i < argv.length; i++) {
      String key = argv[i].replaceFirst("^--", "");
      boolean hasValue = i + 1 < argv.length && !argv[i + 1].startsWith("--");
      if (hasValue) {
        args.put(key, argv[i + 1]);
        i++;
      } else {
        args.put(key, "true");
      }
    }
    return args;
  }

  /**
   * The measured batch. In count mode this is exactly buildExactSequence
   * (precise per-type
   * composition); in duration mode it is a long weighted-random sequence
   * replayed cyclically
   * until the wall-clock window elapses. Both fifo and readwrite replay the
   * SAME measured batch,
   * so the measured per-type counts match exactly.
   */
  private List<Call> buildMeasured(Map<String, String> args,
                                   List<RpcProfile> profiles) {
    long opsTarget = Long.parseLong(args.getOrDefault("ops", "-1"));
    if (opsTarget > 0) {
      return buildExactSequence(computeQuotas(opsTarget, profiles), profiles);
    }
    return generateSequence(2_000_000, profiles);
  }

  /**
   * Count mode: exact #measured calls to consume; <=0 means wall-clock
   * duration mode.
   */
  private long computeStopAfter(Map<String, String> args,
                                List<RpcProfile> profiles) {
    long opsTarget = Long.parseLong(args.getOrDefault("ops", "-1"));
    if (opsTarget > 0) {
      long[] quota = computeQuotas(opsTarget, profiles);
      long s = 0;
      for (long q : quota) {
        s += q;
      }
      return s;
    }
    return -1;
  }

  /**
   * The RPC mix (defaults to a 2-profile READ/WRITE model) derived from args.
   */
  private List<RpcProfile> buildProfiles(Map<String, String> args) {
    String[] ratio = args.getOrDefault("ratio", "1:10").split(":");
    int writeNumerator = Integer.parseInt(ratio[0].trim());
    int readNumerator = Integer.parseInt(ratio[1].trim());
    long readHoldNanos =
        TimeUnit.MICROSECONDS.toNanos(
            Long.parseLong(args.getOrDefault("readHoldMicros", "100")));
    long writeHoldNanos =
        TimeUnit.MICROSECONDS.toNanos(
            Long.parseLong(args.getOrDefault("writeHoldMicros", "200")));
    String mix = args.get("mix");
    if (mix != null && !mix.isEmpty() && !"true".equals(mix)) {
      return parseMix(mix);
    }
    List<RpcProfile> def = new ArrayList<RpcProfile>();
    def.add(new RpcProfile("READ", false, readNumerator, readHoldNanos));
    def.add(new RpcProfile("WRITE", true, writeNumerator, writeHoldNanos));
    return def;
  }

  /** Count mode: #calls to discard as warmup (before the measured window). */
  private long warmupOps(Map<String, String> args) {
    return Long.parseLong(args.getOrDefault("warmupOps", "0"));
  }

  /**
   * Count-mode warmup prefix: a weighted-random batch of warmupOps calls,
   * discarded (not
   * recorded). It is a SEPARATE sequence from the measured batch so that,
   * after it is fully
   * drained, the measured batch can be released as a self-contained unit
   * with exact per-type
   * composition -- independent of readwrite's dequeue reordering. Empty when
   * warmupOps == 0 or in
   * duration mode.
   */
  private List<Call> buildWarmup(Map<String, String> args,
                                 List<RpcProfile> profiles) {
    long warmup = warmupOps(args);
    if (warmup > 0 && Long.parseLong(args.getOrDefault("ops", "-1")) > 0) {
      return generateSequence((int) Math.min(warmup, Integer.MAX_VALUE),
          profiles);
    }
    return Collections.emptyList();
  }



  /**
   * Parses a {@code --mix} spec: comma-separated entries of the form {@code
   * name:R|W:weight:holdMicros}. Example:
   *
   * <pre>--mix
   * "getFileInfo:R:30:10,listStatus:R:5:200,create:W:8:60,
   * addBlock:W:2:150"</pre>
   */
  private List<RpcProfile> parseMix(String mix) {
    List<RpcProfile> out = new ArrayList<RpcProfile>();
    for (String entry : mix.split(",")) {
      String[] f = entry.trim().split(":");
      if (f.length < 4) {
        throw new IllegalArgumentException(
            "mix entry must be name:R|W:weight:holdMicros, got: " + entry);
      }
      String name = f[0].trim();
      boolean write = "W".equalsIgnoreCase(f[1].trim());
      double weight = Double.parseDouble(f[2].trim());
      long hold = TimeUnit.MICROSECONDS.toNanos(Long.parseLong(f[3].trim()));
      out.add(new RpcProfile(name, write, weight, hold));
    }
    if (out.isEmpty()) {
      throw new IllegalArgumentException("empty --mix");
    }
    return out;
  }
  public int run(String[] argv) throws Exception {
    Map<String, String> args = parseArgs(argv);
    int runs = Integer.parseInt(args.getOrDefault("runs", "3"));

    // Mode list: an explicit --mode runs just that mode; otherwise both fifo
    // and
    // readwrite run so a plain invocation is a direct dequeue-order
    // comparison.
    // pass --first readwrite to let readwrite run before fifo (checks warm-up
    // bias).
    List<String> modeList;
    String modeArg = args.get("mode");
    if (modeArg != null && !modeArg.isEmpty()) {
      modeList = Arrays.asList(modeArg.split(","));
    } else if ("readwrite".equalsIgnoreCase(args.getOrDefault("first", ""))) {
      modeList = Arrays.asList("readwrite", "fifo");
    } else {
      modeList = Arrays.asList("fifo", "readwrite");
    }

    // ratio / handlers are comma-separated lists, so a single invocation
    // sweeps many
    // configurations with no dedicated --matrix flag. e.g.
    //   --ratio 1:3,1:8 --handlers 32,64   (full 2x2 sweep, plus both modes)
    String[] ratios = args.getOrDefault("ratio", "1:4").split(",");
    String[] handlersArr = args.getOrDefault("handlers", "64").split(",");

    List<Benchmark> configs = new ArrayList<Benchmark>();
    for (String ratio : ratios) {
      for (String handlers : handlersArr) {
        // Build the deterministic batch ONCE per (ratio, handlers) so the fifo
        // and
        // readwrite Benchmarks in this cell replay the SAME sequence object --
        // guaranteeing identical per-type counts across modes.
        Map<String, String> base = new LinkedHashMap<>(args);
        base.remove("mode");
        base.put("ratio", ratio.trim());
        base.put("handlers", handlers.trim());
        List<RpcProfile> profiles = buildProfiles(base);
        List<Call> warmupSeq = buildWarmup(base, profiles);
        List<Call> measuredSeq = buildMeasured(base, profiles);
        long stopAfter = computeStopAfter(base, profiles);
        for (String mode : modeList) {
          Map<String, String> m = new LinkedHashMap<>(base);
          m.put("mode", mode);
          configs.add(new Benchmark(m, profiles, warmupSeq, measuredSeq,
              stopAfter));
        }
      }
    }

    Benchmark probe = configs.get(0);
    logger.info(probe.header());
    Map<String, List<Benchmark.Result>> byConfig = new LinkedHashMap<>();
    for (Benchmark b : configs) {
      List<Benchmark.Result> results = new ArrayList<>();
      for (int i = 0; i < runs; i++) {
        Benchmark.Result r = b.runOnce();
        results.add(r);
        logger.info(r.csv(b));
      }
      byConfig.put(keyOf(b), results);
    }

    // Median summary, pairing fifo vs readwrite within each (ratio, handlers)
    // group to surface the dequeue-order effect.
    logger.info("=== median summary ===");
    for (String key : byConfig.keySet()) {
      List<Benchmark.Result> rs = byConfig.get(key);
      Benchmark.Result r = median(rs);
      logger.info(
          String.format(
              "%s | %.0f ops/s | rP99=%dus wP99=%dus | wLockBusy=%.1f%% "
                  + "| readsPerRun=%.2f | concurrentReaders=%.2f | "
                  + "rwQueue(r/w)=(%d/%d) phases(r/w)=(%d/%d)%n",
              key,
              r.opsPerSec,
              r.read.waitP99Us,
              r.write.waitP99Us,
              r.writeLockBusyPct,
              r.avgReadsPerRun,
              r.concurrentReadersAvg,
              r.readQueueSize,
              r.writeQueueSize,
              r.readPhaseCount,
              r.writePhaseCount));
    }
    logger.info("=== rpc mix (median, avg hold per type) ===");
    for (String key : byConfig.keySet()) {
      Benchmark.Result r = median(byConfig.get(key));
      StringBuilder sb = new StringBuilder();
      sb.append(key).append(" |");
      for (Map.Entry<String, double[]> e : r.rpcHold.entrySet()) {
        double[] v = e.getValue();
        sb.append(String.format(" %s(count=%.0f,hold=%.1fus) |", e.getKey(),
            v[0], v[1]));
      }
      logger.info(sb.toString());
    }
    // Pairing: for each (ratio, fair, handlers) cell, if BOTH fifo and
    // readwrite
    // were run, print a side-by-side comparison and the readwrite gain. Cells
    // are
    // derived from the actual configs (not hardcoded), so arbitrary --ratio /
    // --handlers lists and --fair values are respected.
    for (Benchmark b : configs) {
      if (!"fifo".equals(b.mode)) {
        continue;
      }
      String ratio = b.writeNumerator + ":" + b.readNumerator;
      String cell = ratio + "," + b.fair + "," + b.handlers;
      Benchmark.Result fifo = median(byConfig.get("fifo," + cell));
      Benchmark.Result rw = median(byConfig.get("readwrite," + cell));
      if (fifo == null || rw == null) {
        continue; // only one mode was requested for this cell
      }
      logger.info(
          String.format(
              "ratio=%s handlers=%s fair=%s | fifo %.0f ops/s ("
                  + "readsPerRun=%.2f rP99=%dus wP99=%dus) | "
                  + "readwrite %.0f ops/s "
                  + "(readsPerRun=%.2f rP99=%dus wP99=%dus) | gain=%+.1f%%%n",
              ratio,
              b.handlers,
              b.fair,
              fifo.opsPerSec,
              fifo.avgReadsPerRun,
              fifo.read.waitP99Us,
              fifo.write.waitP99Us,
              rw.opsPerSec,
              rw.avgReadsPerRun,
              rw.read.waitP99Us,
              rw.write.waitP99Us,
              100.0 * (rw.opsPerSec - fifo.opsPerSec) / fifo.opsPerSec));
    }
    return 0;
  }

  private static String keyOf(Benchmark b) {
    return b.mode
        + ","
        + b.writeNumerator
        + ":"
        + b.readNumerator
        + ","
        + b.fair
        + ","
        + b.handlers;
  }

  private static Benchmark.Result median(List<Benchmark.Result> results) {
    if (results == null || results.isEmpty()) {
      return null;
    }
    List<Benchmark.Result> sorted = new ArrayList<Benchmark.Result>(results);
    sorted.sort(
        new java.util.Comparator<Benchmark.Result>() {
          @Override
          public int compare(Benchmark.Result a, Benchmark.Result b) {
            return Double.compare(a.opsPerSec, b.opsPerSec);
          }
        });
    return sorted.get(sorted.size() / 2);
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new ReadWriteCallQueueBenchmark(), args);
  }
}
