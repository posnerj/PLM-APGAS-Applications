/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package GLBCoopGR;

import static apgas.Constructs.asyncAt;
import static apgas.Constructs.finish;
import static apgas.Constructs.here;
import static apgas.Constructs.place;
import static apgas.Constructs.places;
import static apgas.Constructs.uncountedAsyncAt;

import apgas.SerializableCallable;
import apgas.util.GlobalRef;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import utils.ConsolePrinter;

/**
 * The local runner for the Cooperative.GLBCoopGR framework. An instance of this class runs at each
 * place and provides the context within which user-specified tasks execute and are load balanced
 * across all places.
 *
 * @param <Queue> Concrete TaskQueueGR type
 * @param <T> Result type.
 */
public final class WorkerGR<Queue extends TaskQueueGR<Queue, T>, T extends Serializable>
    implements Serializable {

  private static final long serialVersionUID = 1L;

  final transient AtomicBoolean active = new AtomicBoolean(false);
  final transient AtomicBoolean empty = new AtomicBoolean(true);
  final transient AtomicBoolean waiting = new AtomicBoolean(false);

  /** Number of places. */
  final int P;
  /** TaskQueueGR, responsible for crunching numbers */
  Queue queue;
  /** Read as I am the "lifeline buddy" of my "lifelineThieves" */
  ConcurrentLinkedQueue<Integer> lifelineThieves;
  /** Thieves that send stealing requests */
  FixedSizeStack<Integer> thieves;
  /** Lifeline buddies */
  int[] lifelines;
  /**
   * The data structure to keep a key invariant: At any time, at most one message has been sent on
   * an outgoing lifeline (and hence at most one message has been received on an incoming lifeline).
   */
  boolean[] lifelinesActivated;
  /**
   * The granularity of tasks to run in a batch before starts to probe network to respond to
   * work-stealing requests. The smaller n is, the more responsive to the work-stealing requests; on
   * the other hand, less focused on computation
   */
  int n;
  /** Number of random victims to probe before sending requests to lifeline buddy */
  int w;
  /** Maximum number of random victims */
  int m;
  /**
   * Random number, used when picking a non-lifeline victim/buddy. Important to seed with place id,
   * otherwise BG/Q, the random sequence will be exactly same at different places
   */
  Random random = new Random(here().id);
  /**
   * Random buddies, a runner first probeWs its random buddy, only when none of the random buddies
   * responds it starts to probe its lifeline buddies
   */
  int[] victims;
  /** LoggerGR to record the work-stealing status */
  LoggerGR loggerGR;
  /*
   * printing some helpful output for debugging, default is false
   */
  ConsolePrinter consolePrinter;

  /**
   * Class constructor
   *
   * @param init function closure to init the local {@link TaskQueueGR}
   * @param n same to this.n
   * @param w same to this.w
   * @param m same to this.m
   * @param l power of lifeline graph
   * @param z base of lifeline graph
   * @param tree true if the workload is dynamically generated, false if the workload can be
   *     statically generated
   * @param s true if stopping Time in LoggerGR, false if not
   */
  public WorkerGR(
      SerializableCallable<Queue> init,
      int n,
      int w,
      int l,
      int z,
      int m,
      boolean tree,
      int s,
      int P) {
    this.consolePrinter = ConsolePrinter.getInstance();

    this.P = P;
    this.n = n;
    this.w = w;
    this.m = m;

    this.lifelines = new int[z];
    Arrays.fill(this.lifelines, -1);

    int h = here().id;

    victims = new int[m];
    if (P > 1) {
      for (int i = 0; i < m; i++) {
        while ((victims[i] = random.nextInt(P)) == h) {}
      }
    }

    // lifelines
    int x = 1;
    int t = 0;
    for (int j = 0; j < z; j++) {
      int v = h;
      for (int k = 1; k < l; k++) {
        v = v - v % (x * l) + (v + x * l - x) % (x * l);
        if (v < P) {
          lifelines[t++] = v;
          break;
        }
      }
      x *= l;
    }

    try {
      queue = init.call();
    } catch (Exception e) {
      e.printStackTrace();
    }
    lifelineThieves = new ConcurrentLinkedQueue<>();

    thieves = new FixedSizeStack<Integer>(P, Integer.class);
    lifelinesActivated = new boolean[P];

    if (tree) {
      int[] calculateLifelineThieves = calculateLifelineThieves(l, z, h);
      for (int i : calculateLifelineThieves) {
        if (i != -1) {
          lifelineThieves.add(i);
        }
      }
      for (int i : lifelines) {
        if (i != -1) {
          lifelinesActivated[i] = true;
        }
      }
    }

    loggerGR = new LoggerGR(s);
  }

  /**
   * Internal method used by {@link GLBCoopGR} to start FTWorker at each place when the workload is
   * known statically.
   *
   * @param globalRef GlobalRef of FTWorker
   */
  static <Queue extends TaskQueueGR<Queue, T>, T extends Serializable> void broadcast(
      GlobalRef<WorkerGR<Queue, T>> globalRef) {
    int size = places().size();
    finish(
        () -> {
          if (size < 256) {
            for (int i = 0; i < size; i++) {
              asyncAt(place(i), () -> globalRef.get().main(globalRef));
            }
          } else {
            for (int i = size - 1; i >= 0; i -= 32) {
              asyncAt(
                  place(i),
                  () -> {
                    int max = here().id;
                    int min = Math.max(max - 31, 0);
                    for (int j = min; j <= max; ++j) {
                      asyncAt(place(j), () -> globalRef.get().main(globalRef));
                    }
                  });
            }
          }
        });
  }

  /**
   * Print exceptions
   *
   * @param e exeception
   */
  static void error(Throwable e) {
    System.err.println("Exception at " + here());
    e.printStackTrace();
  }

  // new start distribution with lifeline scheme
  private int[] calculateLifelineThieves(int l, int z, int id) {
    int dim = z;
    int nodecoutPerEdge = l;

    int[] predecessors = new int[dim];
    int mathPower_nodecoutPerEdge_I = 1;
    for (int i = 0; i < dim; i++) {
      int vectorLength = (id / mathPower_nodecoutPerEdge_I) % nodecoutPerEdge;

      if (vectorLength + 1 == nodecoutPerEdge
          || (predecessors[i] = id + mathPower_nodecoutPerEdge_I) >= P) {
        predecessors[i] = id - (vectorLength * mathPower_nodecoutPerEdge_I);

        if (predecessors[i] == id) {
          predecessors[i] = -1;
        }
      }
      mathPower_nodecoutPerEdge_I *= nodecoutPerEdge;
    }
    return predecessors;
  }

  /**
   * Send out the workload to thieves. At this point, either thieves or lifelinetheives is non-empty
   * (or both are non-empty). Note sending workload to the lifeline thieve is the only place that
   * uses async (instead of uncounted async as in other places), which means when only all lifeline
   * requests are responded can the framework be terminated.
   *
   * @param globalRef place local handle of LJR
   * @param loot the taskbag(aka workload) to send out
   */
  public void give(GlobalRef<WorkerGR<Queue, T>> globalRef, TaskBagGR loot) {
    int victim = here().id;
    loggerGR.nodesGiven += loot.size();
    if (thieves.getSize() > 0) {
      final int thief = thieves.pop();
      consolePrinter.println(here() + ": Giving loot to " + thief);
      if (thief >= 0) {
        ++loggerGR.lifelineStealsSuffered;
        uncountedAsyncAt(
            place(thief),
            () -> {
              //                    int newId =
              // globalRef.get().loggerGR.startStoppingTime(LoggerGR.STEALING);
              synchronized (globalRef.get().waiting) {
                globalRef.get().deal(globalRef, loot, victim);

                globalRef
                    .get()
                    .consolePrinter
                    .println(
                        here()
                            + "(in give1): trying to enter synchronized. active = "
                            + globalRef.get().active.get()
                            + "(should be true)");

                globalRef
                    .get()
                    .consolePrinter
                    .println(here() + "(in give1): entered synchronized.");
                globalRef.get().waiting.set(false);
                globalRef.get().waiting.notifyAll();
                //                        globalRef.get().loggerGR.endStoppingTime(newId);
              }
            });
      } else {
        ++loggerGR.stealsSuffered;
        uncountedAsyncAt(
            place(-thief - 1),
            () -> {
              //                    int newId =
              // globalRef.get().loggerGR.startStoppingTime(LoggerGR.STEALING);
              synchronized (globalRef.get().waiting) {
                globalRef.get().deal(globalRef, loot, -1);

                globalRef
                    .get()
                    .consolePrinter
                    .println(
                        here()
                            + "(in give2): trying to enter synchronized. active = "
                            + globalRef.get().active.get()
                            + "(should be true)");

                globalRef
                    .get()
                    .consolePrinter
                    .println(here() + "(in give2): entered synchronized.");
                globalRef.get().waiting.set(false);
                globalRef.get().waiting.notifyAll();
                //                        globalRef.get().loggerGR.endStoppingTime(newId);
              }
            });
      }
    } else {
      ++loggerGR.lifelineStealsSuffered;
      int thief = lifelineThieves.poll();

      asyncAt(
          place(thief),
          () -> {
            //
            // globalRef.get().loggerGR.startStoppingTimeWithAutomaticEnd(LoggerGR.STEALING);
            globalRef
                .get()
                .consolePrinter
                .println(
                    here()
                        + "(in give3): active = "
                        + globalRef.get().active.get()
                        + "(can be both)");
            globalRef.get().deal(globalRef, loot, victim);
          });
    }
  }

  /**
   * Distribute works to (lifeline) thieves by calling the {@link #give(GlobalRef, TaskBagGR)}
   *
   * @param globalRef GlobalRef of FTWorker
   */
  public void distribute(GlobalRef<WorkerGR<Queue, T>> globalRef) {
    if (thieves.getSize() + lifelineThieves.size() > 0) {
      //            loggerGR.startStoppingTimeWithAutomaticEnd(LoggerGR.DISTRIBUTING);
      loggerGR.startStoppingTimeWithAutomaticEnd(LoggerGR.COMMUNICATION);
    }
    TaskBagGR loot;
    while (((thieves.getSize() > 0) || (lifelineThieves.size() > 0))
        && (loot = queue.split()) != null) {
      give(globalRef, loot);
    }
  }

  /**
   * Rejecting thieves when no task to share (or worth sharing). Note, never reject lifeline thief,
   * instead put it into the lifelineThieves stack,
   *
   * @param globalRef GlobalRef of FTWorker
   */
  public void reject(GlobalRef<WorkerGR<Queue, T>> globalRef) {
    this.loggerGR.startStoppingTimeWithAutomaticEnd(LoggerGR.COMMUNICATION);

    while (thieves.getSize() > 0) {
      final int thief = thieves.pop();
      consolePrinter.println(here() + ": rejecting " + thief);
      if (thief >= 0) {
        lifelineThieves.add(thief);
        uncountedAsyncAt(
            place(thief),
            () -> {
              //                    int newId =
              // globalRef.get().loggerGR.startStoppingTime(LoggerGR.STEALING);
              globalRef
                  .get()
                  .consolePrinter
                  .println(here() + "(in reject1): trying to enter synchronized.");
              synchronized (globalRef.get().waiting) {
                int newId = globalRef.get().loggerGR.startStoppingTime(LoggerGR.COMMUNICATION);
                globalRef
                    .get()
                    .consolePrinter
                    .println(here() + "(in reject1): entered synchronized.");
                globalRef.get().waiting.set(false);
                globalRef.get().waiting.notifyAll();
                globalRef.get().loggerGR.endStoppingTime(newId);
              }
            });
      } else {
        uncountedAsyncAt(
            place(-thief - 1),
            () -> {
              //                    int newId =
              // globalRef.get().loggerGR.startStoppingTime(LoggerGR.STEALING);
              globalRef
                  .get()
                  .consolePrinter
                  .println(here() + "(in reject2): trying to enter synchronized.");
              synchronized (globalRef.get().waiting) {
                int newId = globalRef.get().loggerGR.startStoppingTime(LoggerGR.COMMUNICATION);
                globalRef
                    .get()
                    .consolePrinter
                    .println(here() + "(in reject2): entered synchronized.");
                globalRef.get().waiting.set(false);
                globalRef.get().waiting.notifyAll();
                globalRef.get().loggerGR.endStoppingTime(newId);
              }
            });
      }
    }
  }

  /**
   * Send out steal requests. It does following things: (1) Probes w random victims and send out
   * stealing requests by calling into {@link #request(GlobalRef, int, boolean)} (2) If probing
   * random victims fails, resort to lifeline buddies In both case, it sends out the request and
   * wait on the thieves' response, which either comes from (i) {@link #reject(GlobalRef)} method
   * when victim has no workload to share or (ii) {@link #give(GlobalRef, TaskBagGR)}
   *
   * @param globalRef GlobalRef of FTWorker
   * @return !empty.get();
   */
  public boolean steal(GlobalRef<WorkerGR<Queue, T>> globalRef) {
    //        loggerGR.startStoppingTimeWithAutomaticEnd(LoggerGR.STEALING);
    loggerGR.startStoppingTimeWithAutomaticEnd(LoggerGR.COMMUNICATION);
    if (P == 1) {
      return false;
    }
    final int p = here().id;
    for (int i = 0; i < w && empty.get(); ++i) {
      ++loggerGR.stealsAttempted;
      waiting.set(true);
      loggerGR.stopLive();
      int v = victims[random.nextInt(m)];
      try {
        uncountedAsyncAt(
            place(v),
            () -> {
              //                int newId =
              // globalRef.get().loggerGR.startStoppingTime(LoggerGR.DISTRIBUTING);
              globalRef.get().request(globalRef, p, false);
              //                globalRef.get().loggerGR.endStoppingTime(newId);
            });
      } catch (Throwable t) {
        System.out.println(here() + " steal NEW!!!: " + v);
        t.printStackTrace(System.out);
        continue;
      }
      consolePrinter.println(
          here() + "(in steal1): trying to enter synchronized, stole by " + v + ".");
      synchronized (waiting) {
        this.loggerGR.startStoppingTimeWithAutomaticEnd(LoggerGR.WAITING);
        while (waiting.get()) {
          consolePrinter.println(here() + "(in steal1): entered synchronized.");
          try {
            consolePrinter.println(here() + "(in steal1): waiting.");
            waiting.wait();
          } catch (InterruptedException e) {
            System.err.println(here() + ": " + e);
          }
        }
      }
      consolePrinter.println(here() + "(in steal1): exited synchronized.");
      loggerGR.startLive();
    }

    for (int i = 0; (i < lifelines.length) && empty.get() && (0 <= lifelines[i]); ++i) {
      int lifeline = lifelines[i];
      if (!lifelinesActivated[lifeline]) {
        ++loggerGR.lifelineStealsAttempted;
        lifelinesActivated[lifeline] = true;
        waiting.set(true);
        uncountedAsyncAt(
            place(lifeline),
            () -> {
              //                    int newId =
              // globalRef.get().loggerGR.startStoppingTime(LoggerGR.STEALING);
              globalRef.get().request(globalRef, p, true);
              //                    globalRef.get().loggerGR.endStoppingTime(newId);
            });
        consolePrinter.println(here() + "(in steal2): trying to enter synchronized.");
        synchronized (waiting) {
          this.loggerGR.startStoppingTimeWithAutomaticEnd(LoggerGR.WAITING);
          consolePrinter.println(here() + "(in steal2): entered synchronized.");
          while (waiting.get()) {
            try {
              consolePrinter.println(here() + "(in steal2): waiting.");
              waiting.wait();
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        }
        consolePrinter.println(here() + "(in steal2): exited synchronized.");
        loggerGR.startLive();
      }
    }
    return !empty.get();
  }

  /**
   * Remote thief sending requests to local LJR. When empty or waiting for more work, reject
   * non-lifeline thief right away. Note, never reject lifeline thief.
   *
   * @param globalRef PLH for Woker
   * @param thief place id of thief
   * @param lifeline if I am the lifeline buddy of the remote thief
   */
  public void request(GlobalRef<WorkerGR<Queue, T>> globalRef, int thief, boolean lifeline) {
    int newId = loggerGR.startStoppingTime(LoggerGR.COMMUNICATION);

    try {
      if (lifeline) {
        ++loggerGR.lifelineStealsReceived;
      } else {
        ++loggerGR.stealsReceived;
      }
      if (empty.get() || waiting.get()) {
        if (lifeline) {
          lifelineThieves.add(thief);
        }
        consolePrinter.println(here() + ": rejecting " + thief + "(isLifeline = " + lifeline + ")");
        uncountedAsyncAt(
            place(thief),
            () -> {
              //                    int newId =
              // globalRef.get().loggerGR.startStoppingTime(LoggerGR.STEALING);
              globalRef
                  .get()
                  .consolePrinter
                  .println(here() + "(in request): trying to enter synchronized.");
              synchronized (globalRef.get().waiting) {
                int thiefNewId = globalRef.get().loggerGR.startStoppingTime(LoggerGR.COMMUNICATION);
                globalRef
                    .get()
                    .consolePrinter
                    .println(here() + "(in request): entered synchronized.");
                globalRef.get().waiting.set(false);
                globalRef.get().waiting.notifyAll();
                globalRef.get().loggerGR.endStoppingTime(thiefNewId);
              }
            });
      } else {
        consolePrinter.println(
            here()
                + ": pushing "
                + thief
                + "(isLifeline = "
                + lifeline
                + "), empty = "
                + empty.get()
                + ", waiting = "
                + waiting.get()
                + ", active = "
                + active.get());
        if (lifeline) {
          thieves.push(thief);
        } else {
          thieves.push(-thief - 1);
        }
      }
    } catch (Throwable e) {
      error(e);
    }
    this.loggerGR.endStoppingTime(newId);
  }

  /**
   * Merge current WorkerGR'timestamps taskbag with incoming task bag.
   *
   * @param loot task bag to merge
   * @param lifeline if it is from a lifeline buddy
   */
  public void processLoot(TaskBagGR loot, boolean lifeline) {

    if (lifeline) {
      ++loggerGR.lifelineStealsPerpetrated;
      loggerGR.lifelineNodesReceived += loot.size();
    } else {
      ++loggerGR.stealsPerpetrated;
      loggerGR.nodesReceived += loot.size();
    }
    queue.merge(loot);
    empty.set(false);
  }

  /**
   * Main process function of FTWorker. It does 4 things: (1) execute at most n tasks (2) respond to
   * stealing requests (3) when not worth sharing tasks, reject the stealing requests (4) when
   * running out of tasks, steal from others
   *
   * @param globalRef the GlobalRef of FTWorker
   */
  public void processStack(GlobalRef<WorkerGR<Queue, T>> globalRef) {
    consolePrinter.println(here() + " starts in processStack");
    loggerGR.startStoppingTimeWithAutomaticEnd(LoggerGR.COMPUTING);
    //    loggerGR.startStoppingTimeWithAutomaticEnd(LoggerGR.PROCESSING);
    boolean cont;
    int size;
    consolePrinter.println(here() + " partial result1: " + queue.getResult().getResult()[0]);
    do {
      boolean process = true;
      do {
        synchronized (waiting) {
          loggerGR.startStoppingTimeWithAutomaticEnd(LoggerGR.PROCESSING);
          process = queue.process(n);
          distribute(globalRef);
        }

        //        loggerGR.startStoppingTimeWithAutomaticEnd(LoggerGR.COMPUTING);
        reject(globalRef);
      } while (process);
      empty.set(true);
      reject(globalRef);
      consolePrinter.println(here() + "(in processStack): trying to enter synchronized.");
      consolePrinter.println(here() + " partial result2: " + queue.getResult().getResult()[0]);
      synchronized (waiting) {
        consolePrinter.println(here() + "(in processStack): entered synchronized.");
        cont = steal(globalRef) || 0 < queue.size();
        loggerGR.startStoppingTimeWithAutomaticEnd(LoggerGR.COMPUTING);
        //        loggerGR.startStoppingTimeWithAutomaticEnd(LoggerGR.PROCESSING);
        this.active.set(cont);
        size = this.queue.size();
      }
    } while (cont);
    if (0 < size) {
      System.err.println(here() + "size is " + size + ", but should be 0!");
    }
    reject(globalRef);
    loggerGR.startStoppingTimeWithAutomaticEnd(LoggerGR.IDLING);
    reject(globalRef);
    loggerGR.startStoppingTimeWithAutomaticEnd(LoggerGR.IDLING);
  }

  /**
   * Entry point when workload is only known dynamically . The workflow is terminated when (1) No
   * one has work to do (2) Lifeline steals are responded
   *
   * @param ref local handle for FTWorker
   * @param start init method used in {@link TaskQueueGR}, note the workload is not allocated,
   *     because the workload can only be self-generated.
   */
  public void main(GlobalRef<WorkerGR<Queue, T>> ref, Runnable start) {
    consolePrinter.println(here() + " main1");
    finish(
        () -> {
          try {
            empty.set(false);
            active.set(true);
            loggerGR.startLive();
            start.run();
            processStack(ref);
            loggerGR.endStoppingTimeWithAutomaticEnd();
            loggerGR.stopLive();
            loggerGR.nodesCount = queue.count();
          } catch (Throwable t) {
            error(t);
          }
        });
  }

  /**
   * Entry point when workload can be known statically. The workflow is terminated when (1) No one
   * has work to do (2) Lifeline steals are responded
   *
   * @param ref handle for FTWorker. Note the workload is assumed to be allocated already in the
   *     {@link TaskQueueGR} constructor.
   */
  public void main(GlobalRef<WorkerGR<Queue, T>> ref) {
    consolePrinter.println(here() + " main2");
    try {
      empty.set(false);
      active.set(true);
      loggerGR.startLive();
      processStack(ref);
      loggerGR.endStoppingTimeWithAutomaticEnd();
      loggerGR.stopLive();
      loggerGR.nodesCount = queue.count();
    } catch (Throwable t) {
      error(t);
    }
  }

  /**
   * Deal workload to the theif. If the thief is active already, simply merge the taskbag. If the
   * thief is inactive, the thief gets reactiveated again.
   *
   * @param st: PLH for FTWorker
   * @param loot Task to share
   * @param source victim id
   */
  private void deal(GlobalRef<WorkerGR<Queue, T>> st, TaskBagGR loot, int source) {
    this.loggerGR.startStoppingTimeWithAutomaticEnd(LoggerGR.COMMUNICATION);

    try {
      boolean oldActive;
      consolePrinter.println(here() + "(in deal): trying to enter synchronized.");
      synchronized (waiting) {
        consolePrinter.println(here() + "(in deal): entered synchronized.");
        boolean lifeline = source >= 0;
        if (lifeline) {
          lifelinesActivated[source] = false;
        }
        oldActive = this.active.getAndSet(true);

        if (oldActive) {
          processLoot(loot, lifeline);
        } else {
          loggerGR.startLive();
          processLoot(loot, lifeline);
        }
      }

      if (!oldActive) {
        consolePrinter.println(here() + "(in deal): restarted");
        processStack(st);
        loggerGR.endStoppingTimeWithAutomaticEnd();
        loggerGR.stopLive();
        loggerGR.nodesCount = queue.count();
      }
    } catch (Throwable t) {
      error(t);
    }
  }
}
