package GLBCoop;

import static apgas.Constructs.asyncAt;
import static apgas.Constructs.finish;
import static apgas.Constructs.here;
import static apgas.Constructs.place;
import static apgas.Constructs.places;
import static apgas.Constructs.uncountedAsyncAt;

import apgas.SerializableCallable;
import apgas.util.PlaceLocalObject;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import utils.ConsolePrinter;

/**
 * The local runner for the Cooperative.GLBCoop framework. An instance of this class runs at each
 * place and provides the context within which user-specified tasks execute and are load balanced
 * across all places.
 *
 * @param <Queue> Concrete TaskQueue type
 * @param <T> Result type.
 */
public final class Worker<Queue extends TaskQueue<Queue, T>, T extends Serializable>
    extends PlaceLocalObject implements Serializable {

  private static final long serialVersionUID = 1L;

  final transient AtomicBoolean active = new AtomicBoolean(false);
  final transient AtomicBoolean empty = new AtomicBoolean(true);
  final transient AtomicBoolean waiting = new AtomicBoolean(false);

  /** Number of places. */
  final int P;
  /** TaskQueue, responsible for crunching numbers */
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
  /** Logger to record the work-stealing status */
  Logger logger;
  /*
   * printing some helpful output for debugging, default is false
   */
  ConsolePrinter consolePrinter;

  /**
   * Class constructor
   *
   * @param init function closure to init the local {@link TaskQueue}
   * @param n same to this.n
   * @param w same to this.w
   * @param m same to this.m
   * @param l power of lifeline graph
   * @param z base of lifeline graph
   * @param tree true if the workload is dynamically generated, false if the workload can be
   *     statically generated
   * @param s true if stopping Time in Logger, false if not
   */
  public Worker(
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

    logger = new Logger(s);
  }

  /**
   * Internal method used by {@link GLBCoop} to start FTWorker at each place when the workload is
   * known statically.
   */
  static <Queue extends TaskQueue<Queue, T>, T extends Serializable> void broadcast(
      Worker<Queue, T> worker) {
    int size = places().size();
    finish(
        () -> {
          if (size < 256) {
            for (int i = 0; i < size; i++) {
              asyncAt(place(i), worker::main);
            }
          } else {
            for (int i = size - 1; i >= 0; i -= 32) {
              asyncAt(
                  place(i),
                  () -> {
                    int max = here().id;
                    int min = Math.max(max - 31, 0);
                    for (int j = min; j <= max; ++j) {
                      asyncAt(place(j), worker::main);
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
   * @param loot the taskbag(aka workload) to send out
   */
  public void give(TaskBag loot) {
    int victim = here().id;
    logger.nodesGiven += loot.size();
    if (thieves.getSize() > 0) {
      final int thief = thieves.pop();
      consolePrinter.println(here() + ": Giving loot to " + thief);
      if (thief >= 0) {
        ++logger.lifelineStealsSuffered;
        uncountedAsyncAt(
            place(thief),
            () -> {
              //              int newId = this.logger.startStoppingTime(Logger.STEALING);
              synchronized (this.waiting) {
                deal(loot, victim);

                this.consolePrinter.println(
                    here()
                        + "(in give1): trying to enter synchronized. active = "
                        + this.active.get()
                        + "(should be true)");

                this.consolePrinter.println(here() + "(in give1): entered synchronized.");
                this.waiting.set(false);
                this.waiting.notifyAll();
                //                this.logger.endStoppingTime(newId);
              }
            });
      } else {
        ++logger.stealsSuffered;
        uncountedAsyncAt(
            place(-thief - 1),
            () -> {
              //              int newId = this.logger.startStoppingTime(Logger.STEALING);
              synchronized (this.waiting) {
                deal(loot, -1);

                this.consolePrinter.println(
                    here()
                        + "(in give2): trying to enter synchronized. active = "
                        + this.active.get()
                        + "(should be true)");

                this.consolePrinter.println(here() + "(in give2): entered synchronized.");
                this.waiting.set(false);
                this.waiting.notifyAll();
                //                this.logger.endStoppingTime(newId);
              }
            });
      }
    } else {
      ++logger.lifelineStealsSuffered;
      int thief = lifelineThieves.poll();

      asyncAt(
          place(thief),
          () -> {
            //            this.logger.startStoppingTimeWithAutomaticEnd(Logger.STEALING);
            this.consolePrinter.println(
                here() + "(in give3): active = " + this.active.get() + "(can be both)");
            deal(loot, victim);
          });
    }
  }

  /** Distribute works to (lifeline) thieves by calling the {@link #give(TaskBag)} */
  public void distribute() {
    if (thieves.getSize() + lifelineThieves.size() > 0) {
      //            logger.startStoppingTimeWithAutomaticEnd(Logger.DISTRIBUTING);
      logger.startStoppingTimeWithAutomaticEnd(Logger.COMMUNICATION);
    }
    TaskBag loot;
    while (((thieves.getSize() > 0) || (lifelineThieves.size() > 0))
        && (loot = queue.split()) != null) {
      give(loot);
    }
  }

  /**
   * Rejecting thieves when no task to share (or worth sharing). Note, never reject lifeline thief,
   * instead put it into the lifelineThieves stack,
   */
  public void reject() {
    this.logger.startStoppingTimeWithAutomaticEnd(Logger.COMMUNICATION);

    while (thieves.getSize() > 0) {
      final int thief = thieves.pop();
      consolePrinter.println(here() + ": rejecting " + thief);
      if (thief >= 0) {
        lifelineThieves.add(thief);
        uncountedAsyncAt(
            place(thief),
            () -> {
              //              int newId = this.logger.startStoppingTime(Logger.STEALING);
              this.consolePrinter.println(here() + "(in reject1): trying to enter synchronized.");
              synchronized (this.waiting) {
                int newId = this.logger.startStoppingTime(Logger.COMMUNICATION);
                this.consolePrinter.println(here() + "(in reject1): entered synchronized.");
                this.waiting.set(false);
                this.waiting.notifyAll();
                this.logger.endStoppingTime(newId);
              }
            });
      } else {
        uncountedAsyncAt(
            place(-thief - 1),
            () -> {
              //              int newId = this.logger.startStoppingTime(Logger.STEALING);
              this.consolePrinter.println(here() + "(in reject2): trying to enter synchronized.");
              synchronized (this.waiting) {
                int newId = this.logger.startStoppingTime(Logger.COMMUNICATION);
                this.consolePrinter.println(here() + "(in reject2): entered synchronized.");
                this.waiting.set(false);
                this.waiting.notifyAll();
                this.logger.endStoppingTime(newId);
              }
            });
      }
    }
  }

  /**
   * Send out steal requests. It does following things: (1) Probes w random victims and send out
   * stealing requests by calling into {@link #request(int, boolean)} (2) If probing random victims
   * fails, resort to lifeline buddies In both case, it sends out the request and wait on the
   * thieves' response, which either comes from (i) {@link #reject()} method when victim has no
   * workload to share or (ii) {@link #give(TaskBag)}
   *
   * @return !empty.get();
   */
  public boolean steal() {
    //        logger.startStoppingTimeWithAutomaticEnd(Logger.STEALING);
    logger.startStoppingTimeWithAutomaticEnd(Logger.COMMUNICATION);
    if (P == 1) {
      return false;
    }
    final int p = here().id;
    for (int i = 0; i < w && empty.get(); ++i) {
      ++logger.stealsAttempted;
      waiting.set(true);
      logger.stopLive();
      int v = victims[random.nextInt(m)];
      try {
        uncountedAsyncAt(
            place(v),
            () -> {
              //              int newId = this.logger.startStoppingTime(Logger.DISTRIBUTING);
              request(p, false);
              //              this.logger.endStoppingTime(newId);
            });
      } catch (Throwable t) {
        System.out.println(here() + " steal NEW!!!: " + v);
        t.printStackTrace(System.out);
        continue;
      }
      consolePrinter.println(
          here() + "(in steal1): trying to enter synchronized, stole by " + v + ".");
      synchronized (waiting) {
        this.logger.startStoppingTimeWithAutomaticEnd(Logger.WAITING);
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
      logger.startLive();
    }

    for (int i = 0; (i < lifelines.length) && empty.get() && (0 <= lifelines[i]); ++i) {
      int lifeline = lifelines[i];
      if (!lifelinesActivated[lifeline]) {
        ++logger.lifelineStealsAttempted;
        lifelinesActivated[lifeline] = true;
        waiting.set(true);
        uncountedAsyncAt(
            place(lifeline),
            () -> {
              //              int newId = this.logger.startStoppingTime(Logger.STEALING);
              request(p, true);
              //              this.logger.endStoppingTime(newId);
            });
        consolePrinter.println(here() + "(in steal2): trying to enter synchronized.");
        synchronized (waiting) {
          this.logger.startStoppingTimeWithAutomaticEnd(Logger.WAITING);
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
        logger.startLive();
      }
    }
    return !empty.get();
  }

  /**
   * Remote thief sending requests to local LJR. When empty or waiting for more work, reject
   * non-lifeline thief right away. Note, never reject lifeline thief.
   *
   * @param thief place id of thief
   * @param lifeline if I am the lifeline buddy of the remote thief
   */
  public void request(int thief, boolean lifeline) {
    int newId = logger.startStoppingTime(Logger.COMMUNICATION);

    try {
      if (lifeline) {
        ++logger.lifelineStealsReceived;
      } else {
        ++logger.stealsReceived;
      }
      if (empty.get() || waiting.get()) {
        if (lifeline) {
          lifelineThieves.add(thief);
        }
        consolePrinter.println(here() + ": rejecting " + thief + "(isLifeline = " + lifeline + ")");
        uncountedAsyncAt(
            place(thief),
            () -> {
              //              int newId = this.logger.startStoppingTime(Logger.STEALING);
              this.consolePrinter.println(here() + "(in request): trying to enter synchronized.");
              synchronized (this.waiting) {
                int thiefNewId = this.logger.startStoppingTime(Logger.COMMUNICATION);
                this.consolePrinter.println(here() + "(in request): entered synchronized.");
                this.waiting.set(false);
                this.waiting.notifyAll();
                this.logger.endStoppingTime(thiefNewId);
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
    this.logger.endStoppingTime(newId);
  }

  /**
   * Merge current Worker'timestamps taskbag with incoming task bag.
   *
   * @param loot task bag to merge
   * @param lifeline if it is from a lifeline buddy
   */
  public void processLoot(TaskBag loot, boolean lifeline) {
    if (lifeline) {
      ++logger.lifelineStealsPerpetrated;
      logger.lifelineNodesReceived += loot.size();
    } else {
      ++logger.stealsPerpetrated;
      logger.nodesReceived += loot.size();
    }
    queue.merge(loot);
    empty.set(false);
  }

  /**
   * Main process function of FTWorker. It does 4 things: (1) execute at most n tasks (2) respond to
   * stealing requests (3) when not worth sharing tasks, reject the stealing requests (4) when
   * running out of tasks, steal from others
   */
  public void processStack() {
    consolePrinter.println(here() + " starts in processStack");
    logger.startStoppingTimeWithAutomaticEnd(Logger.COMPUTING);
    //    logger.startStoppingTimeWithAutomaticEnd(Logger.PROCESSING);
    boolean cont;
    int size;
    consolePrinter.println(here() + " partial result1: " + queue.getResult().getResult()[0]);
    do {
      boolean process = true;
      do {
        synchronized (waiting) {
          logger.startStoppingTimeWithAutomaticEnd(Logger.PROCESSING);
          process = queue.process(n);
          distribute();
        }

        //        logger.startStoppingTimeWithAutomaticEnd(Logger.COMPUTING);
        reject();
      } while (process);
      empty.set(true);
      reject();
      consolePrinter.println(here() + "(in processStack): trying to enter synchronized.");
      consolePrinter.println(here() + " partial result2: " + queue.getResult().getResult()[0]);
      synchronized (waiting) {
        consolePrinter.println(here() + "(in processStack): entered synchronized.");
        cont = steal() || 0 < queue.size();
        logger.startStoppingTimeWithAutomaticEnd(Logger.COMPUTING);
        //        logger.startStoppingTimeWithAutomaticEnd(Logger.PROCESSING);
        this.active.set(cont);
        size = this.queue.size();
      }
    } while (cont);
    if (0 < size) {
      System.err.println(here() + "size is " + size + ", but should be 0!");
    }
    reject();
    logger.startStoppingTimeWithAutomaticEnd(Logger.IDLING);
    reject();
    logger.startStoppingTimeWithAutomaticEnd(Logger.IDLING);
  }

  /**
   * Entry point when workload is only known dynamically . The workflow is terminated when (1) No
   * one has work to do (2) Lifeline steals are responded
   *
   * @param start init method used in {@link TaskQueue}, note the workload is not allocated, because
   *     the workload can only be self-generated.
   */
  public void main(Runnable start) {
    consolePrinter.println(here() + " main1");
    finish(
        () -> {
          try {
            empty.set(false);
            active.set(true);
            logger.startLive();
            start.run();
            processStack();
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
   */
  public void main() {
    consolePrinter.println(here() + " main2");
    try {
      empty.set(false);
      active.set(true);
      logger.startLive();
      processStack();
      logger.endStoppingTimeWithAutomaticEnd();
      logger.stopLive();
      logger.nodesCount = queue.count();
    } catch (Throwable t) {
      error(t);
    }
  }

  /**
   * Deal workload to the theif. If the thief is active already, simply merge the taskbag. If the
   * thief is inactive, the thief gets reactiveated again.
   *
   * @param loot Task to share
   * @param source victim id
   */
  private void deal(TaskBag loot, int source) {
    this.logger.startStoppingTimeWithAutomaticEnd(Logger.COMMUNICATION);

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
          logger.startLive();
          processLoot(loot, lifeline);
        }
      }

      if (!oldActive) {
        consolePrinter.println(here() + "(in deal): restarted");
        processStack();
        logger.endStoppingTimeWithAutomaticEnd();
        logger.stopLive();
        logger.nodesCount = queue.count();
      }
    } catch (Throwable t) {
      error(t);
    }
  }
}
