/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package GLBCoord;

import static apgas.Constructs.asyncAt;
import static apgas.Constructs.at;
import static apgas.Constructs.finish;
import static apgas.Constructs.here;
import static apgas.Constructs.place;
import static apgas.Constructs.places;

import GLBCoop.Logger;
import GLBCoop.TaskBag;
import apgas.Place;
import apgas.SerializableCallable;
import apgas.util.GlobalRef;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import utils.ConsolePrinter;

/**
 * The local runner for the GLBCoord.SplitGLB framework. An instance of this class runs at each
 * place and provides the context within which user-specified tasks execute and are load balanced
 * across all places.
 *
 * @param <Queue> Concrete TaskQueue type
 * @param <T> Result type.
 */
public final class WorkerCoord<Queue extends TaskQueueCoord<Queue, T>, T extends Serializable>
    implements Serializable {

  private static final long serialVersionUID = 1L;

  final transient AtomicBoolean active = new AtomicBoolean(false);
  final transient AtomicBoolean empty = new AtomicBoolean(true);
  final transient AtomicBoolean waiting = new AtomicBoolean(false);

  /** TaskQueue, responsible for crunching numbers */
  Queue queue;

  /** Read as I am the "lifeline buddy" of my "lifelineThieves" */
  ConcurrentLinkedQueue<Integer> lifelineThieves;

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
   * Random buddies, a runner first probes its random buddy, only when none of the random buddies
   * responds it starts to probe its lifeline buddies
   */
  int[] victims;

  /** Logger to record the work-stealing status */
  Logger logger;

  /* Number of places.*/
  int P = places().size();

  /*
   * printing some helpful output for debugging, default is false
   */
  ConsolePrinter consolePrinter;

  /**
   * Class constructor
   *
   * @param n same to this.n
   * @param w same to this.w
   * @param m same to this.m
   * @param l power of lifeline graph
   * @param z base of lifeline graph
   * @param tree true if the workload is dynamically generated, false if the workload can be
   *     statically generated
   * @param s true if stopping Time in Logger, false if not
   */
  public WorkerCoord(
      SerializableCallable<Queue> init, int n, int w, int l, int z, int m, boolean tree, int s) {
    this.consolePrinter = ConsolePrinter.getInstance();

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
    logger = new Logger(s);
  }

  /**
   * Internal method used by {@link GLBCoord} to start FTWorker at each place when the workload is
   * known statically.
   *
   * @param globalRef GlobalRef of FTWorker
   */
  static <Queue extends TaskQueueCoord<Queue, T>, T extends Serializable> void broadcast(
      GlobalRef<WorkerCoord<Queue, T>> globalRef) {
    int size = places().size();
    finish(
        () -> {
          if (size < 256) {
            for (int i = 0; i < size; i++) {
              asyncAt(places().get(i), () -> globalRef.get().main(globalRef));
            }
          } else {
            for (int i = size - 1; i >= 0; i -= 32) {
              asyncAt(
                  places().get(i),
                  () -> {
                    int max = here().id;
                    int min = Math.max(max - 31, 0);
                    for (int j = min; j <= max; ++j) {
                      asyncAt(places().get(j), () -> globalRef.get().main(globalRef));
                    }
                  });
            }
          }
        });
  }

  /**
   * Print exceptions
   *
   * @param e exception
   */
  static void error(Throwable e) {
    System.err.println("Exception at " + here());
    e.printStackTrace();
  }

  // new start distribution with lifeline scheme
  private int[] calculateLifelineThieves(int l, int z, int id) {
    int dim = z;
    int nodecountPerEdge = l;

    int[] predecessors = new int[dim];
    int mathPower_nodecountPerEdge_I = 1;
    for (int i = 0; i < dim; i++) {
      int vectorLength = (id / mathPower_nodecountPerEdge_I) % nodecountPerEdge;

      if (vectorLength + 1 == nodecountPerEdge
          || (predecessors[i] = id + mathPower_nodecountPerEdge_I) >= P) {
        predecessors[i] = id - (vectorLength * mathPower_nodecountPerEdge_I);

        if (predecessors[i] == id) {
          predecessors[i] = -1;
        }
      }
      mathPower_nodecountPerEdge_I *= nodecountPerEdge;
    }
    return predecessors;
  }

  /**
   * Send out the workload to thieves. At this point, either thieves or lifelinethieves is non-empty
   * (or both are non-empty). Note sending workload to the lifeline thieve is the only place that
   * uses async (instead of uncounted async as in other places), which means when only all lifeline
   * requests are responded can the framework be terminated.
   *
   * @param globalRef place local handle of LJR
   * @param loot the taskbag(aka workload) to send out
   */
  public void give(GlobalRef<WorkerCoord<Queue, T>> globalRef, TaskBag loot, Integer thief) {

    this.queue.release(); // cannot be within split, because split is called by not-owners.
    int victim = here().id;
    logger.nodesGiven += loot.size();
    ++logger.lifelineStealsSuffered;
    asyncAt(
        places().get(thief),
        () -> {
          globalRef.get().logger.startStoppingTimeWithAutomaticEnd(Logger.STEALING);
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

  /**
   * Distribute works to (lifeline) thieves by calling the {@link #(GlobalRef, TaskBag)}
   *
   * @param globalRef GlobalRef of FTWorker
   */
  public void distribute(GlobalRef<WorkerCoord<Queue, T>> globalRef) {
    //        if (thieves.size() + lifelineThieves.size() > 0) {//paper
    if (lifelineThieves.isEmpty() == false) {
      logger.startStoppingTimeWithAutomaticEnd(Logger.DISTRIBUTING);
    }
    TaskBag loot = null;
    Integer thief;
    while ((((thief = lifelineThieves.poll()) != null)) && (loot = queue.split()) != null) {
      consolePrinter.println(here() + "starting give");
      give(globalRef, loot, thief);
      consolePrinter.println(here() + "end give");
    }

    if (thief != null && loot == null) {
      lifelineThieves.add(thief);
    }
  }

  /**
   * Send out steal requests. It does following things: (1) Probes w random victims and send out
   * stealing requests by calling into (2) If probing random victims fails, resort to lifeline
   * buddies In both case, it sends out the request and wait on the thieves' response, which either
   * comes from
   *
   * @param globalRef GlobalRef of FTWorker
   * @return !empty.get();
   */
  public boolean steal(GlobalRef<WorkerCoord<Queue, T>> globalRef) {
    logger.startStoppingTimeWithAutomaticEnd(Logger.STEALING);
    if (P == 1) {
      return false;
    }
    for (int i = 0; i < w && empty.get(); ++i) {
      ++logger.stealsAttempted;
      logger.stopLive();
      int v = victims[random.nextInt(m)];
      final TaskBag[] taskBag = new TaskBag[1];
      consolePrinter.println(here() + "(vor random steal): finish " + places().get(v));

      taskBag[0] =
          at(
              place(v),
              () -> {
                int newId = globalRef.get().logger.startStoppingTime(Logger.DISTRIBUTING);
                final TaskBag split = globalRef.get().queue.split();
                globalRef.get().logger.endStoppingTime(newId);
                if (split != null && split.size() > 0) {
                  globalRef.get().logger.stealsSuffered++;
                  globalRef.get().logger.nodesGiven += split.size();
                }
                return split;
              });

      consolePrinter.println(here() + "(nach random steal): finish " + places().get(v));
      if (taskBag[0] != null && taskBag[0].size() > 0) {
        this.logger.stealsPerpetrated++;
        this.logger.nodesReceived += taskBag[0].size();
        this.queue.mergePublic(taskBag[0]);
        empty.set(false);
      }

      logger.startLive();
    }

    // lifeline steals
    for (int i = 0; (i < lifelines.length) && empty.get() && (0 <= lifelines[i]); ++i) {
      int lifeline = lifelines[i];
      if (!lifelinesActivated[lifeline]) {
        ++logger.lifelineStealsAttempted;
        lifelinesActivated[lifeline] = true;
        logger.stopLive();
        Place lifelineVictim = places().get(lifeline);
        final TaskBag[] taskBag = new TaskBag[1];
        final int lifelineThief = here().id;
        consolePrinter.println(here() + "(vor lifeline steal): finish " + lifelineVictim);

        taskBag[0] =
            at(
                lifelineVictim,
                () -> {
                  int newId = globalRef.get().logger.startStoppingTime(Logger.DISTRIBUTING);
                  final TaskBag split = globalRef.get().queue.split();
                  globalRef.get().logger.endStoppingTime(newId);
                  if (split != null && split.size() > 0) {
                    globalRef.get().logger.lifelineStealsSuffered++;
                    globalRef.get().logger.nodesGiven += split.size();
                  } else {
                    globalRef.get().lifelineThieves.add(lifelineThief);
                  }
                  return split;
                });

        consolePrinter.println(here() + "(nach lifeline steal): finish " + lifelineVictim);
        if (taskBag[0] != null && taskBag[0].size() > 0) {
          this.logger.lifelineStealsPerpetrated++;
          this.logger.lifelineNodesReceived += taskBag[0].size();
          this.queue.mergePublic(taskBag[0]);
          empty.set(false);
          lifelinesActivated[lifeline] = false;
        }
        logger.startLive();
      }
    }
    return !empty.get();
  }

  /**
   * Main process function of FTWorker. It does 4 things: (1) execute at most n tasks (2) respond to
   * stealing requests (3) when not worth sharing tasks, reject the stealing requests (4) when
   * running out of tasks, steal from others
   *
   * @param globalRef the GlobalRef of FTWorker
   */
  public void processStack(GlobalRef<WorkerCoord<Queue, T>> globalRef) {
    logger.startStoppingTimeWithAutomaticEnd(Logger.COMPUTING);
    boolean cont = false;
    int size;
    int counter = 0;

    int newN = 200;
    int k = 0;

    do {
      while (queue.process(n)) {
        this.queue.release();
        distribute(globalRef);
        logger.startStoppingTimeWithAutomaticEnd(Logger.COMPUTING);
      }
      empty.set(true);
      consolePrinter.println(here() + "(in processStack): trying to enter synchronized.");
      consolePrinter.println(here() + "(in processStack): entered synchronized.");
      steal(globalRef);
      logger.startStoppingTimeWithAutomaticEnd(Logger.COMPUTING);
      consolePrinter.println(
          here()
              + "(middle_1 of processStack), "
              + "cont: "
              + cont
              + ", size of q: "
              + queue.size()
              + ", private: "
              + this.queue.privateSize()
              + ", public: "
              + this.queue.publicSize());
      cont = queue.size() > 0;
      consolePrinter.println(
          here()
              + "(middle_2 of processStack), "
              + " "
              + "cont: "
              + cont
              + ", size of q: "
              + queue.size()
              + ", private: "
              + this.queue.privateSize()
              + ", public: "
              + this.queue.publicSize());
      this.active.set(cont);
      size = this.queue.size();
    } while (cont);
    if (0 < size) {
      System.err.println(here() + "size is " + size + ", but should be 0!");
    }
    consolePrinter.println(
        here()
            + "(end of processStack), size of lootMerge: "
            + ", size of lifelineThieves: "
            + lifelineThieves.size()
            + ", sizeOfElements: "
            + globalRef.get().queue.getElementsSize()
            + ", countRelease: "
            + this.queue.getCountRelease()
            + ", countReacquire: "
            + this.queue.getCountRequire());
    logger.startStoppingTimeWithAutomaticEnd(Logger.DEAD);
  }

  /**
   * Entry point when workload is only known dynamically . The workflow is terminated when (1) No
   * one has work to do (2) Lifeline steals are responded
   *
   * @param st local handle for FTWorker the workload can only be self-generated.
   */
  public void main(GlobalRef<WorkerCoord<Queue, T>> st, Runnable start) {
    finish(
        () -> {
          try {
            empty.set(false);
            active.set(true);
            logger.startLive();
            start.run();
            processStack(st);
            logger.endStoppingTimeWithAutomaticEnd();
            logger.stopLive();
            logger.nodesCount = queue.count();
          } catch (Throwable t) {
            error(t);
          }
        });
  }

  /**
   * Entry point when workload can be known statically. The workflow is terminated when (1) No one
   * has work to do (2) Lifeline steals are responded
   *
   * <p>constructor.
   */
  public void main(GlobalRef<WorkerCoord<Queue, T>> st) {
    try {
      empty.set(false);
      active.set(true);
      logger.startLive();
      processStack(st);
      logger.endStoppingTimeWithAutomaticEnd();
      logger.stopLive();
      logger.nodesCount = queue.count();
    } catch (Throwable t) {
      error(t);
    }
  }

  /**
   * Deal workload to the thief. If the thief is active already, simply merge the taskbag. If the
   * thief is inactive, the thief gets reactiveated again.
   *
   * @param st: PLH for FTWorker
   * @param loot Task to share
   * @param source victim id
   */
  private void deal(GlobalRef<WorkerCoord<Queue, T>> st, TaskBag loot, int source) {
    try {
      boolean oldActive;
      consolePrinter.println(here() + "(in deal): trying to enter synchronized.");
      synchronized (waiting) {
        consolePrinter.println(here() + "(in deal): entered synchronized.");
        lifelinesActivated[source] = false;
        oldActive = this.active.getAndSet(true);
        this.queue.mergePublic(loot);
        this.empty.set(false);
        if (!oldActive) {
          logger.startLive();
        }
      }

      if (!oldActive) {
        consolePrinter.println(here() + "(in deal): restarted");
        processStack(st);
        logger.endStoppingTimeWithAutomaticEnd();
        logger.nodesCount = queue.count();
        logger.stopLive();
      }
    } catch (Throwable t) {
      error(t);
    }
  }
}
