package IncFTGLB;

import static apgas.Constructs.async;
import static apgas.Constructs.asyncAt;
import static apgas.Constructs.at;
import static apgas.Constructs.finish;
import static apgas.Constructs.here;
import static apgas.Constructs.immediateAsyncAt;
import static apgas.Constructs.isDead;
import static apgas.Constructs.place;
import static apgas.Constructs.places;
import static apgas.Constructs.prevPlace;
import static apgas.Constructs.uncountedAsyncAt;

import apgas.DeadPlacesException;
import apgas.GlobalRuntime;
import apgas.Place;
import apgas.SerializableCallable;
import apgas.util.ExactlyOnceExecutor;
import apgas.util.PlaceLocalObject;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTaskContext;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import utils.ConsolePrinter;
import utils.Pair;

/**
 * The local runner for the Cooperative.FTGLB framework. An instance of this class runs at each
 * place and provides the context within which user-specified tasks execute and are load balanced
 * across all places.
 *
 * @param <Queue> Concrete TaskQueue type
 * @param <T> Result type.
 */
public final class IncFTWorker<Queue extends IncFTTaskQueue<Queue, T>, T extends Serializable>
    extends PlaceLocalObject implements Serializable {

  private static final long serialVersionUID = 1L;
  /** the resilient map for backups */
  protected final transient IMap<Integer, IncQueueWrapper<Queue, T>> iMapBackup;

  protected final String iMapBackupHandlerRemoveID;
  /** given loot without answer */
  protected final transient IMap<Integer, HashMap<Integer, Pair<Long, IncTaskBag>>> iMapOpenLoot;

  protected final String iMapOpenLootHandlerRemoveID;
  /** TaskQueue, responsible for crunching numbers */
  final Queue queue;

  final IncQueueWrapper<Queue, T> queueWrapper;
  /** ExactlyOnceExecutor for exactly-once policy of the EntryProcessors */
  private final transient ExactlyOnceExecutor exactlyOnceExecutor = new ExactlyOnceExecutor();
  /** hazelcast instance */
  private final transient HazelcastInstance hz = Hazelcast.getHazelcastInstanceByName("apgas");
  /** number of places at start */
  private final int startPlacesSize;
  /**
   * Random number, used when picking a non-lifeline victim/buddy. Important to seed with place id,
   * otherwise BG/Q, the random sequence will be exactly same at different places
   */
  private final Random random = new Random(here().id);
  /** Cycle of writing regular backups */
  private final int k;
  /** states of worker */
  private final transient AtomicBoolean active = new AtomicBoolean(false);

  private final transient AtomicBoolean empty = new AtomicBoolean(true);
  private final transient AtomicBoolean waiting = new AtomicBoolean(false);
  /** Read as I am the "lifeline buddy" of my "lifelineThieves" */
  private final LinkedList<Integer> lifelineThieves;
  /**
   * Have I wait for the finish of the last put working? Could be boolean but object is required for
   * syn (and Boolean is not suitable for that)
   */
  private final AtomicBoolean waitingForPutWorking;
  /** Structures for restartDaemon, only used on place 0 */
  private final HashMap<Integer, Boolean> workingPlaces;

  private final ConcurrentLinkedQueue<Integer> restartPlaces;
  private final HashMap<Integer, HashMap<Integer, Boolean>> countHandler;
  private final HashMap<Integer, Integer> placeKeyMap;
  /** Crash number for testing purposes. */
  private final int crashNumber;
  /** Logger to record the work-stealing status */
  IncFTLogger logger;
  /** Unique Number for loot */
  private long currentLid;
  /** Thieves that send stealing requests */
  private LinkedList<Integer> thieves;
  /** Lifeline buddies */
  private int[] lifelines;
  /**
   * The data structure to keep a key invariant: At any time, at most one message has been sent on
   * an outgoing lifeline (and hence at most one message has been received on an incoming lifeline).
   */
  private boolean[] lifelinesActivated;
  /**
   * The granularity of tasks to run in a batch before starts to probe network to respond to
   * work-stealing requests. The smaller n is, the more responsive to the work-stealing requests; on
   * the other hand, less focused on computation
   */
  private int n;
  /** Number of random victims to probe before sending requests to lifeline buddy */
  private int w;
  /** Maximum number of random victims */
  private int m;
  /**
   * Random buddies, a runner first probeWs its random buddy, only when none of the random buddies
   * responds it starts to probe its lifeline buddies
   */
  private int[] victims;
  /** Prints some helpful output for debugging, default is false */
  private transient ConsolePrinter consolePrinter = ConsolePrinter.getInstance();
  //    private final IAtomicLong iCurrentLid;
  /** Thieves that send stealing requests */
  private ArrayList<Integer> deadPlaces;
  /** Waiting for response of this place */
  private int waitingPlace;
  /** Name of current iMapBackup for backups */
  private String backupMapName;
  /** Name of current iMapBackup for openLoots */
  private String openLootMapName;
  /** Write cyclic backups every k * n computation-elements. */
  private int currentK;
  /**
   * Used for incremental backups Snapshot of stable tasks in current interval This snapshot will be
   * merged into the backup on the next call of writeBackup.
   */
  private Snapshot<T> snap = new Snapshot<T>();

  /** Used for incremental backups Contains the count of tasks in the backup. */
  private long lastSnapTasks = 0;

  private long workerStartTime;

  /**
   * Class constructor
   *
   * @param init function closure to init the local {@link IncFTWorker}
   * @param n same to this.n
   * @param w same to this.w
   * @param m same to this.m
   * @param l power of lifeline graph
   * @param z base of lifeline graph
   * @param tree true if the workload is dynamically generatedf, false if the workload can be
   *     statically generated
   * @param s true if stopping Time in Logger, false if not
   */
  IncFTWorker(
      SerializableCallable<Queue> init,
      int n,
      int w,
      int l,
      int z,
      int m,
      boolean tree,
      int s,
      int k,
      int crashNumber,
      int backupCount,
      int P,
      HashMap<Integer, Integer> placeKeyMap)
      throws Exception {
    this.consolePrinter.println("begin constructor");
    this.startPlacesSize = P;
    this.placeKeyMap = placeKeyMap;

    this.n = n;
    this.w = w;
    this.m = m;
    this.k = k;
    this.waitingForPutWorking = new AtomicBoolean(false);
    this.currentLid = Long.MIN_VALUE;
    this.currentLid++;
    this.logger = new IncFTLogger(s);

    this.crashNumber = crashNumber;

    // only used on place 0
    this.workingPlaces = new HashMap<>();
    this.restartPlaces = new ConcurrentLinkedQueue<>();
    this.countHandler = new HashMap<>();
    if (here().id == 0) {
      for (int i = 0; i < this.startPlacesSize; i++) {
        this.workingPlaces.put(i, false);
      }
    }

    this.openLootMapName = "iMapOpenLoot";

    MapConfig openLootMapConfig = new MapConfig(this.openLootMapName);
    openLootMapConfig.setBackupCount(backupCount);
    this.hz.getConfig().addMapConfig(openLootMapConfig);

    this.iMapOpenLoot = this.hz.getMap(this.openLootMapName);
    this.iMapOpenLootHandlerRemoveID =
        this.iMapOpenLoot.addPartitionLostListener(
            event -> {
              System.out.println(
                  here()
                      + "(in partitionLostListener - iMapOpenLoot): "
                      + event.toString()
                      + " ...shutting down the cluster now, result will be wrong");
              this.killAllPlaces();
            });

    if (here().id == 0) {
      for (int i = 0; i < this.startPlacesSize; i++) {
        HashMap<Integer, Pair<Long, IncTaskBag>> newHashMap = new HashMap<>();
        this.iMapOpenLoot.set(getBackupKey(i), newHashMap);
      }
    }

    this.lifelines = new int[z];
    Arrays.fill(this.lifelines, -1);

    int h = here().id;

    this.victims = new int[m];
    this.consolePrinter.println("calculate victims");
    if (startPlacesSize > 1) {
      for (int i = 0; i < m; i++) {
        while ((this.victims[i] = this.random.nextInt(startPlacesSize)) == h) {}
      }
    }

    this.consolePrinter.println("before lifelines");
    // lifelines
    int x = 1;
    int t = 0;
    for (int j = 0; j < z; j++) {
      int v = h;
      for (int u = 1; u < l; u++) {
        v = v - v % (x * l) + (v + x * l - x) % (x * l);
        if (v < startPlacesSize) {
          lifelines[t++] = v;
          break;
        }
      }
      x *= l;
    }
    this.consolePrinter.println("After lifelines");

    this.backupMapName = "iMapBackup";

    MapConfig backupMapConfig = new MapConfig(this.backupMapName);
    backupMapConfig.setBackupCount(backupCount);
    backupMapConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
    this.hz.getConfig().addMapConfig(backupMapConfig);

    this.iMapBackup = hz.getMap(this.backupMapName);

    this.iMapBackupHandlerRemoveID =
        this.iMapBackup.addPartitionLostListener(
            event -> {
              System.out.println(
                  here()
                      + "(in partitionLostListener - iMapBackup): "
                      + event.toString()
                      + " ...shutting down the cluster now, result will be wrong");
              this.killAllPlaces();
            });

    this.queue = init.call();
    this.queueWrapper = new IncQueueWrapper<Queue, T>(queue, startPlacesSize);

    this.iMapBackup.set(getBackupKey(here().id), this.queueWrapper);
    this.logger.regularBackupsWritten++;
    takeSnapshot();
    lastSnapTasks = snap.minS + snap.taskA.size();

    this.lifelineThieves = new LinkedList<>();

    this.thieves = new LinkedList<>();
    this.lifelinesActivated = new boolean[startPlacesSize];

    synchronized (this.lifelineThieves) {
      if (tree) {
        int[] calculateLifelineThieves = calculateLifelineThieves(l, z, h);
        for (int i : calculateLifelineThieves) {
          if (i != -1) {
            this.lifelineThieves.add(i);
          }
        }
        for (int i : this.lifelines) {
          if (i != -1) {
            this.lifelinesActivated[i] = true;
          }
        }
      }
    }
    this.waitingPlace = -1;
    this.deadPlaces = new ArrayList<>();
    GlobalRuntime.getRuntime().setPlaceFailureHandler(this::placeFailureHandler);
    GlobalRuntime.getRuntime()
        .setRuntimeShutdownHandler(
            () -> {
              GlobalRuntime.getRuntime().setPlaceFailureHandler(null);
              System.out.println(
                  here()
                      + " APGAS-Runtime is shutting down "
                      + "...shutting down the cluster now. "
                      + "This should only happen after the whole computation, "
                      + "otherwise the result will be wrong.");
              for (Place place : places()) { // assume runtime can still distribute tasks
                try {
                  if (place != here()) {
                    immediateAsyncAt(place, () -> System.exit(57));
                  }
                  System.exit(57);
                } catch (Throwable throwable) {
                }
              }
            });
    this.consolePrinter.println("end constructor");
  }

  /**
   * Internal method used by {@link IncFTGLB} to start FTWorker at each place when the workload is
   * known statically.
   */
  static <Queue extends IncFTTaskQueue<Queue, T>, T extends Serializable> void broadcast(
      IncFTWorker<Queue, T> worker) {
    final int size = places().size();
    try {
      finish(
          () -> {
            // for restarting places in this finish
            asyncAt(place(0), worker::restartDaemon);

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
    } catch (DeadPlacesException e) {
      System.out.println(here() + "(in broadcast): DeadPlacesException caught");
      e.printStackTrace(System.out);
    }
  }

  /** Used for incremental backups Takes a snapshot of the stable tasks. */
  private void takeSnapshot() {
    this.consolePrinter.println("Trying to enter this.waiting");
    synchronized (this.waiting) {
      this.consolePrinter.println("Entered this.waiting");
      snap.minS = Math.max(queue.size() - 1, 0);
      snap.taskA = queue.getTopElement();
      snap.result = queue.getResult();
      snap.count = queue.count();
    }
    this.consolePrinter.println("Left this.waiting");
  }

  private int getBackupKey(int placeID) {
    return this.placeKeyMap.get((placeID + 1) % startPlacesSize);
  }

  /**
   * New start distribution with lifeline scheme
   *
   * @param l l
   * @param z z
   * @param id id
   * @return lifelinescheme
   */
  private int[] calculateLifelineThieves(int l, int z, int id) {
    int[] predecessors = new int[z];
    int mathPower_nodecoutPerEdge_I = 1;
    for (int i = 0; i < z; i++) {
      int vectorLength = (id / mathPower_nodecoutPerEdge_I) % l;

      if (vectorLength + 1 == l
          || (predecessors[i] = id + mathPower_nodecoutPerEdge_I) >= startPlacesSize) {
        predecessors[i] = id - (vectorLength * mathPower_nodecoutPerEdge_I);

        if (predecessors[i] == id) {
          predecessors[i] = -1;
        }
      }
      mathPower_nodecoutPerEdge_I *= l;
    }
    return predecessors;
  }

  /**
   * Is triggered from APGAS if a place is crashed
   *
   * @param deadPlace crashed Place
   */
  private void placeFailureHandler(final Place deadPlace) {
    this.consolePrinter.println("begin deadPlace=" + deadPlace);

    final Place placeFailureHandlerPlace = here();

    if (deadPlace.id == 0) {
      this.killAllPlaces();
      return;
    }

    if (crashNumber == 10 && here().id == 1) {
      try {
        Thread.sleep(120_000);
      } catch (InterruptedException e1) {
        e1.printStackTrace();
      }
    }

    boolean isBackupPlace = false;
    boolean oldActive = true;
    boolean merged = false;

    this.consolePrinter.println("Trying to enter this.waiting");
    synchronized (this.waiting) {
      this.consolePrinter.println("Entered this.waiting");
      if (this.deadPlaces.contains(deadPlace.id)) {
        System.out.println(
            here()
                + "(in placeFailureHandler): detected failure of "
                + deadPlace
                + ", but already handled, should never happen!");
        return;
      } else {
        this.deadPlaces.add(deadPlace.id);
      }

      if (here().id == 0) {
        this.putWorkingPlaces(deadPlace, false);
      }

      // refresh countHandler
      final ArrayList<Integer> deadPlacesFromFirstHandler = this.deadPlaces;
      try {
        uncountedAsyncAt(
            place(0),
            () -> {
              this.consolePrinter.println(
                  "uncountedAsyncAt refresh countHandler before synchronized! deadPlace.id="
                      + deadPlace.id);
              synchronized (this.countHandler) {
                this.consolePrinter.println(
                    "uncountedAsyncAt refresh countHandler in synchronized! deadPlace.id="
                        + deadPlace.id);
                if (this.countHandler.containsKey(deadPlace.id) == false) {
                  HashMap<Integer, Boolean> openHandlerList = new HashMap<>();
                  for (Place p : places()) {
                    openHandlerList.put(p.id, false);
                  }
                  for (int deadPlacesId : deadPlacesFromFirstHandler) {
                    openHandlerList.put(deadPlacesId, true);
                  }
                  this.countHandler.put(deadPlace.id, openHandlerList);
                  this.consolePrinter.println("set countHandler of " + deadPlace.id);
                }
                for (Integer key : this.countHandler.keySet()) {
                  if (key == deadPlace.id) {
                    continue;
                  }
                  for (Integer deadPlacesId : deadPlacesFromFirstHandler) {
                    this.countHandler.get(key).put(deadPlacesId, true);
                  }
                }
              }
              this.consolePrinter.println("uncountedAsyncAt after countHandler!");
            });
      } catch (Throwable t) {
        t.printStackTrace(System.out);
      }

      this.consolePrinter.println("Trying to enter this.lifelineThieves");
      synchronized (this.lifelineThieves) {
        this.consolePrinter.println("Entered this.lifelineThieves");
        boolean removeLifelineThieves = this.lifelineThieves.remove(Integer.valueOf(deadPlace.id));
        boolean removeLifeLineFromThieves = this.thieves.remove(Integer.valueOf(deadPlace.id));
        boolean removeThieves = this.thieves.remove(Integer.valueOf(-deadPlace.id - 1));
      }
      this.consolePrinter.println("Left this.lifelineThieves");

      // refresh lifelines
      boolean isLifeline = false;
      for (int lifeline : this.lifelines) {
        if (lifeline == deadPlace.id) {
          isLifeline = true;
          break;
        }
      }

      if (isLifeline == true) {
        int[] tmpLifelines = new int[this.lifelines.length - 1];
        int j = 0;
        for (int lifeline : this.lifelines) {
          if (lifeline != deadPlace.id) {
            tmpLifelines[j] = lifeline;
            j++;
          }
        }
        this.lifelines = tmpLifelines;
      }
      // for final termination all lifelines has to be true
      this.lifelinesActivated[deadPlace.id] = true;

      // refresh victims
      ArrayList<Integer> tmpList = new ArrayList<>();
      for (int victim : this.victims) {
        if (victim != deadPlace.id) {
          tmpList.add(victim);
        }
      }
      if (tmpList.size() > 0) {
        this.victims = new int[tmpList.size()];
        for (int i = 0; i < tmpList.size(); i++) {
          this.victims[i] = tmpList.get(i);
        }
      }
      int oldM = this.m;
      this.m = tmpList.size();
      if (this.waitingPlace == deadPlace.id) {
        this.consolePrinter.println("Trying to enter this.waiting waitingPlace==deadPlace");
        synchronized (this.waiting) {
          this.consolePrinter.println("Entered this.waiting waitingPlace==deadPlace");
          this.waiting.set(false);
          this.waiting.notifyAll();
        }
        this.consolePrinter.println("Left this.waiting waitingPlace==deadPlace");
        this.waitingPlace = -1;
      }

      Place prev = null;
      try {
        prev = prevPlace(deadPlace);
      } catch (Throwable throwable) {
        throwable.printStackTrace(System.out);
      }

      if (prev.id == here().id) {
        Place iteratorDeadPlace = place((here().id + 1) % this.startPlacesSize);
        do {
          // make sure backups of iteratorDeadPlace are consistent
          while (!this.hz.getPartitionService().isClusterSafe()) {
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }

          this.consolePrinter.println("Trying to restore place " + iteratorDeadPlace.id);
          isBackupPlace |= restoreBackup(iteratorDeadPlace);

          boolean foundLoot = false;
          final HashMap<Integer, Pair<Long, IncTaskBag>> deadPlaceOpenLoot =
              this.iMapOpenLoot.get(getBackupKey(iteratorDeadPlace.id));
          for (Pair<Long, IncTaskBag> pair : deadPlaceOpenLoot.values()) {
            if (pair != null) {
              foundLoot = true;
              break;
            }
          }

          if (foundLoot == false) {
            this.consolePrinter.println("No OpenLoot found!");
            iteratorDeadPlace = place((iteratorDeadPlace.id + 1) % this.startPlacesSize);
            continue;
          }

          // openLoot was found
          this.consolePrinter.println("OpenLoot of place " + iteratorDeadPlace.id + " was found");
          for (Map.Entry<Integer, Pair<Long, IncTaskBag>> entry : deadPlaceOpenLoot.entrySet()) {
            if (entry.getValue() == null) {
              continue;
            }
            final int thiefOfDeadPlaceID = entry.getKey();
            final Place thiefOfDeadPlace = place(thiefOfDeadPlaceID);
            final Long deadLid = entry.getValue().getFirst();
            final IncTaskBag deadLoot = entry.getValue().getSecond();
            final int iteratorDeadPlaceID = iteratorDeadPlace.id;

            this.consolePrinter.println(
                "OpenLoot size()=" + deadLoot.size() + " thiefOfDeadPlaceID=" + thiefOfDeadPlaceID);

            if (isDead(thiefOfDeadPlace)) {
              this.consolePrinter.println("Thief " + thiefOfDeadPlace + " is dead!");
              if (this.iMapBackup
                  .get(getBackupKey(thiefOfDeadPlaceID))
                  .getReceivedLid(iteratorDeadPlaceID)
                  >= deadLid
                  || this.iMapBackup.get(getBackupKey(iteratorDeadPlaceID)).getMyLid() < deadLid) {
                this.consolePrinter.println("OpenLoot is already merged into backup!");
                deadPlaceOpenLoot.put(thiefOfDeadPlaceID, null);
                this.iMapOpenLoot.set(getBackupKey(iteratorDeadPlaceID), deadPlaceOpenLoot);
              } else {
                this.consolePrinter.println(
                    "OpenLoot not merged into backup! Merge into own queue!");
                try {
                  TransactionOptions txOptions =
                      new TransactionOptions().setTimeout(15, TimeUnit.SECONDS);
                  this.hz.executeTransaction(
                      txOptions,
                      (TransactionalTaskContext context) -> {
                        final TransactionalMap<Integer, IncQueueWrapper<Queue, T>> transBackupMap =
                            context.getMap(this.backupMapName);
                        final TransactionalMap<Integer, HashMap<Integer, Pair<Long, IncTaskBag>>>
                            transOpenLootMap = context.getMap(this.openLootMapName);
                        this.queue.mergeAtBottom(deadLoot);
                        crashPlace(11, 1, 0, deadPlace.id, deadLid);
                        transBackupMap.set(getBackupKey(here().id), this.queueWrapper);
                        takeSnapshot();
                        lastSnapTasks = snap.minS + snap.taskA.size();
                        this.logger.stealBackupsWritten++;
                        this.currentK = 0;
                        deadPlaceOpenLoot.put(thiefOfDeadPlaceID, null);
                        transOpenLootMap.set(getBackupKey(iteratorDeadPlaceID), deadPlaceOpenLoot);

                        IncQueueWrapper<Queue, T> deadPlaceBackup =
                            transBackupMap.getForUpdate(getBackupKey(iteratorDeadPlaceID));
                        deadPlaceBackup.setDone(true);
                        this.consolePrinter.println(
                            "Dead-Thief-Transaction iteratorDeadPlaceID="
                                + iteratorDeadPlaceID
                                + " deadPlaceBackup.queue.size()="
                                + deadPlaceBackup.queue.size());
                        transBackupMap.set(getBackupKey(iteratorDeadPlaceID), deadPlaceBackup);
                        return null;
                      });
                  merged = true;
                  this.consolePrinter.println("Merged!");
                } catch (TransactionException transException) {
                  transException.printStackTrace(System.out);
                } catch (Throwable t) {
                  t.printStackTrace(System.out);
                }
              }
            } else {
              this.consolePrinter.println(
                  "Thief " + thiefOfDeadPlace + " is alive! Call dealCalledFromHandler!");
              try {
                boolean thiefOldActive = true;
                if (thiefOfDeadPlaceID == here().id) {
                  thiefOldActive =
                      this.dealCalledFromHandler(deadLoot, iteratorDeadPlaceID, deadLid);
                  merged = true;
                } else {
                  thiefOldActive =
                      at(
                          thiefOfDeadPlace,
                          () -> {
                            return this.dealCalledFromHandler(
                                deadLoot, iteratorDeadPlaceID, deadLid);
                          });
                }

                this.consolePrinter.println("dealCalledFromHandler returned. Write iMapOpenLoot");

                deadPlaceOpenLoot.put(thiefOfDeadPlaceID, null);
                this.iMapOpenLoot.set(getBackupKey(iteratorDeadPlaceID), deadPlaceOpenLoot);

                this.consolePrinter.println("thiefOldActive=" + thiefOldActive);
                if (thiefOldActive == false) {
                  this.restartPlaceWithDaemon(thiefOfDeadPlace);
                }
                this.consolePrinter.println("dealCalledFromHandler finished without error!");

              } catch (Throwable t) {
                t.printStackTrace(System.out);
                this.consolePrinter.println("dealCalledFromHandler crashed! Merge into own queue!");
                this.consolePrinter.println(
                    "iMapBackup.get(here().id).queue.size = "
                        + iMapBackup.get(getBackupKey(here().id)).queue.size());

                crashPlace(7, 1, 0, thiefOfDeadPlace.id, deadLid);

                if (this.iMapBackup
                    .get(getBackupKey(thiefOfDeadPlaceID))
                    .getReceivedLid(iteratorDeadPlaceID)
                    >= deadLid
                    || this.iMapBackup.get(getBackupKey(iteratorDeadPlaceID)).getMyLid()
                    < deadLid) {
                  this.consolePrinter.println("OpenLoot is already merged into backup!");
                  deadPlaceOpenLoot.put(thiefOfDeadPlaceID, null);
                  this.iMapOpenLoot.set(getBackupKey(iteratorDeadPlaceID), deadPlaceOpenLoot);
                } else {
                  this.consolePrinter.println(
                      "OpenLoot not merged into backup! Merge into own queue!");
                  try {
                    TransactionOptions txOptions =
                        new TransactionOptions().setTimeout(15, TimeUnit.SECONDS);
                    this.hz.executeTransaction(
                        txOptions,
                        (TransactionalTaskContext context) -> {
                          final TransactionalMap<Integer, IncQueueWrapper<Queue, T>>
                              transBackupMap = context.getMap(this.backupMapName);
                          final TransactionalMap<Integer, HashMap<Integer, Pair<Long, IncTaskBag>>>
                              transOpenLootMap = context.getMap(this.openLootMapName);

                          this.queue.mergeAtBottom(deadLoot);
                          crashPlace(11, 1, 0, deadPlace.id, deadLid);
                          transBackupMap.set(getBackupKey(here().id), this.queueWrapper);
                          takeSnapshot();
                          lastSnapTasks = snap.minS + snap.taskA.size();
                          this.logger.stealBackupsWritten++;
                          this.currentK = 0;

                          deadPlaceOpenLoot.put(thiefOfDeadPlaceID, null);
                          transOpenLootMap.set(
                              getBackupKey(iteratorDeadPlaceID), deadPlaceOpenLoot);

                          IncQueueWrapper<Queue, T> deadPlaceBackup =
                              transBackupMap.getForUpdate(getBackupKey(iteratorDeadPlaceID));
                          this.consolePrinter.println(
                              "Crashed-Thief-Transaction iteratorDeadPlaceID="
                                  + iteratorDeadPlaceID
                                  + " deadPlaceBackup.queue.size()="
                                  + deadPlaceBackup.queue.size());
                          deadPlaceBackup.setDone(true);
                          transBackupMap.set(getBackupKey(iteratorDeadPlaceID), deadPlaceBackup);
                          return null;
                        });
                    merged = true;
                    this.consolePrinter.println("Merged!");
                  } catch (TransactionException transException) {
                    transException.printStackTrace(System.out);
                  } catch (Throwable throwable) {
                    throwable.printStackTrace(System.out);
                  }
                }
              }
            }
          }
          iteratorDeadPlace = place((iteratorDeadPlace.id + 1) % this.startPlacesSize);
        } while (isDead(iteratorDeadPlace));

        if (isBackupPlace == true || merged == true) {
          this.empty.set(false);
          oldActive = this.active.getAndSet(true);
        }
      }

      try {
        HashMap<Integer, Pair<Long, IncTaskBag>> integerArrayListHashMap =
            this.iMapOpenLoot.get(getBackupKey(here().id));
        Pair<Long, IncTaskBag> deadPlaceLoot = integerArrayListHashMap.get(deadPlace.id);

        if (deadPlaceLoot != null && deadPlaceLoot.getFirst() > Long.MIN_VALUE) {
          this.consolePrinter.println("There is loot sent to dead place but was not confirmed!");
          final long backupLid =
              this.iMapBackup.get(getBackupKey(deadPlace.id)).getReceivedLid(here().id);

          if (deadPlaceLoot.getFirst() > backupLid) {
            this.consolePrinter.println("Loot is not in backup. Merge into own queue!");
            this.queue.mergeAtBottom(deadPlaceLoot.getSecond());
            takeSnapshot();
            lastSnapTasks = snap.minS + snap.taskA.size();
            this.logger.stealBackupsWritten++;
            this.currentK = 0;
            while (true) { // try until transaction is executed
              try {
                TransactionOptions txOptions =
                    new TransactionOptions().setTimeout(15, TimeUnit.SECONDS);
                this.hz.executeTransaction(
                    txOptions,
                    (TransactionalTaskContext context) -> {
                      final TransactionalMap<Integer, IncQueueWrapper<Queue, T>> transBackupMap =
                          context.getMap(this.backupMapName);
                      final TransactionalMap<Integer, HashMap<Integer, Pair<Long, IncTaskBag>>>
                          transOpenLootMap = context.getMap(this.openLootMapName);

                      IncQueueWrapper<Queue, T> deadPlaceBackup =
                          transBackupMap.getForUpdate(getBackupKey(deadPlace.id));
                      this.consolePrinter.println(
                          "Sent-Loot-Transaction deadPlace.id="
                              + deadPlace.id
                              + " deadPlaceBackup.queue.size()="
                              + deadPlaceBackup.queue.size());
                      deadPlaceBackup.setDone(true);
                      transBackupMap.set(getBackupKey(deadPlace.id), deadPlaceBackup);

                      transBackupMap.set(getBackupKey(here().id), this.queueWrapper);
                      integerArrayListHashMap.put(deadPlace.id, null);
                      transOpenLootMap.set(getBackupKey(here().id), integerArrayListHashMap);
                      return null;
                    });
                merged = true;
                this.consolePrinter.println("Merged!");
                break;
              } catch (TransactionException transException) {
                transException.printStackTrace(System.out);
              } catch (Throwable t) {
                t.printStackTrace(System.out);
              }
            }
          } else {
            this.consolePrinter.println("Loot is already in backup!");
            integerArrayListHashMap.put(deadPlace.id, null);
            this.iMapOpenLoot.set(getBackupKey(here().id), integerArrayListHashMap);
            this.consolePrinter.println("Removed OpenLoot!");
          }

          if (merged == true) {
            this.empty.set(false);
            if (oldActive == true) {
              oldActive = this.active.getAndSet(true);
            } else {
              this.active.set(true);
            }
          }
        }
      } catch (Throwable e) {
        e.printStackTrace(System.out);
      }

      this.consolePrinter.println(
          "deadPlace.id="
              + deadPlace.id
              + " isBackupPlace="
              + isBackupPlace
              + " merged="
              + merged
              + " oldActive="
              + oldActive);
      if ((isBackupPlace == true || merged == true) && oldActive == false) {
        this.restartPlaceWithDaemon(here());
      }

      if (here().id == 0) {
        this.consolePrinter.println("Waiting for refresh of countHandler!");
        // waiting for delayed refresh of countHandler
        while (this.countHandler.containsKey(deadPlace.id) == false) {
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
        this.countHandler.get(deadPlace.id).put(here().id, true);
        this.consolePrinter.println("Count Handler updated!");
      } else {
        try {
          uncountedAsyncAt(
              place(0),
              () -> {
                this.consolePrinter.println(
                    "uncountedAsyncAt waiting for refresh of countHandler!");
                // waiting for delayed refresh of countHandler
                while (this.countHandler.containsKey(deadPlace.id) == false) {
                  try {
                    Thread.sleep(500);
                  } catch (InterruptedException e) {
                    e.printStackTrace();
                  }
                }
                this.countHandler.get(deadPlace.id).put(placeFailureHandlerPlace.id, true);
                this.consolePrinter.println("uncountedAsyncAt Count Handler updated!");
              });
        } catch (Throwable throwable) {
        }
      }
    }
    this.consolePrinter.println("Left this.waiting");
    this.consolePrinter.println("end deadPlace=" + deadPlace);
  }

  /**
   * Send notice to restartDeamon to restart place Only called from placeFailureHandler
   *
   * @param place place to restart
   */
  private void restartPlaceWithDaemon(final Place place) {
    this.consolePrinter.println("begin place=" + place);

    final int h = place.id;
    final Place p = place;

    if (here().id == 0) {
      if (isDead(p)) {
        return;
      }
      this.restartPlaces.add(h);

      this.consolePrinter.println("Trying to enter this.workingPlaces");
      synchronized (this.workingPlaces) {
        this.consolePrinter.println("Entered this.workingPlaces");
        this.workingPlaces.notifyAll();
      }
      this.consolePrinter.println("Left this.workingPlaces");
    } else {
      try {
        uncountedAsyncAt(
            place(0),
            () -> {
              if (isDead(p)) {
                return;
              }

              this.restartPlaces.add(h);

              this.consolePrinter.println("Trying to enter this.workingPlaces");
              synchronized (this.workingPlaces) {
                this.consolePrinter.println("Entered this.workingPlaces");
                this.workingPlaces.notifyAll();
              }
              this.consolePrinter.println("Left this.workingPlaces");
            });
      } catch (Throwable throwable) {
        throwable.printStackTrace(System.out);
      }
    }
    this.consolePrinter.println("end place=" + place);
  }

  /**
   * Restores a backup
   *
   * @param restorePlace place which will be restored
   * @return really restored a backup?
   */
  private boolean restoreBackup(Place restorePlace) {
    this.consolePrinter.println("begin restoreBackup place=" + restorePlace);

    boolean merged = false;

    while (true) { // try until transaction is executed
      try {
        TransactionOptions txOptions = new TransactionOptions().setTimeout(15, TimeUnit.SECONDS);
        IncTaskBag mergedTasks =
            this.hz.executeTransaction(
                txOptions,
                (TransactionalTaskContext context) -> {
                  final TransactionalMap<Integer, IncQueueWrapper<Queue, T>> transBackupMap =
                      context.getMap(this.backupMapName);

                  IncQueueWrapper<Queue, T> deadQueue =
                      transBackupMap.getForUpdate(getBackupKey(restorePlace.id));
                  this.consolePrinter.println(
                      "Before remove. deadQueue.size() = "
                          + deadQueue.queue.size()
                          + " this.queue.size()="
                          + this.queue.size());
                  IncTaskBag allTasks = deadQueue.queue.removeFromBottom(deadQueue.queue.size());
                  deadQueue.setDone(true);
                  transBackupMap.set(getBackupKey(restorePlace.id), deadQueue);

                  IncQueueWrapper<Queue, T> backupQueueWrapper =
                      transBackupMap.getForUpdate(getBackupKey(here().id));
                  this.consolePrinter.println(
                      "Before merge backupQueueWrapper.queue.size()="
                          + backupQueueWrapper.queue.size()
                          + " allTasks="
                          + allTasks.size());
                  backupQueueWrapper.queue.mergeAtBottom(allTasks);
                  this.consolePrinter.println(
                      "After merge backupQueueWrapper.queue.size()="
                          + backupQueueWrapper.queue.size()
                          + " allTasks="
                          + allTasks.size());
                  transBackupMap.set(getBackupKey(here().id), backupQueueWrapper);

                  this.consolePrinter.println(
                      "Merged "
                          + allTasks.size()
                          + " tasks! deadQueue.size()="
                          + deadQueue.queue.size()
                          + " this.queue.size()="
                          + this.queue.size());
                  return allTasks;
                });
        this.queue.mergeAtBottom(mergedTasks);
        int taskSize = mergedTasks.size();
        this.snap.minS += taskSize;
        if (this.snap.taskA.size() == 0 && this.queue.size() > 0) {
          --this.snap.minS;
          this.snap.taskA = this.queue.getFromBottom(1, this.snap.minS);
        }
        this.lastSnapTasks += taskSize;
        this.consolePrinter.println("Now really merged with queue.size()=" + this.queue.size());
        if (this.queue.size() > 0 && mergedTasks.size() > 0) {
          this.consolePrinter.println("result=true");
          merged = true;
        }
        this.currentK = 0;
        break;
      } catch (TransactionException transException) {
        this.consolePrinter.println("Transaction Exception!");
        transException.printStackTrace(System.out);
      } catch (Throwable throwable) {
        this.consolePrinter.println("Transaction Throwable!");
        throwable.printStackTrace(System.out);
      }
    }

    this.consolePrinter.println(
        "end restoreBackup this.queue.size() = " + this.queue.size() + " place=" + restorePlace);
    return merged;
  }

  /**
   * Send out the workload to thieves. At this point, either thieves or lifeline thieves is
   * non-empty (or both are non-empty). Note sending workload to the lifeline thieve is the only
   * place that uses async (instead of uncounted async as in other places), which means when only
   * all lifeline requests are responded can the framework be terminated.
   *
   * @param loot the Taskbag(aka workload) to send out
   */
  public void give(final IncTaskBag loot) {
    this.consolePrinter.println("begin loot.size=" + loot.size());

    final long lid = this.currentLid++;
    final int victim = here().id;
    logger.nodesGiven += loot.size();
    final int h = here().id;

    if (this.thieves.size() > 0) {
      final Integer thief = this.thieves.poll();
      if (thief == null) {
        System.out.println(here() + "(in give): thief is null, should never happen!!! lid " + lid);
        this.queue.mergeAtBottom(loot);
        return;
      }
      this.consolePrinter.println("random thief=" + thief);

      final int t = thief < 0 ? -thief - 1 : thief;

      try {
        HashMap<Integer, Pair<Long, IncTaskBag>> hashMap = this.iMapOpenLoot.get(getBackupKey(h));
        Pair<Long, IncTaskBag> pair = new Pair<>(lid, loot);
        if (hashMap.get(t) != null) {
          System.out.println(
              here()
                  + "(in give): want to write openLoot, but is not null, should never happen!!!, thief: "
                  + t
                  + ", lid "
                  + lid);
        }
        hashMap.put(t, pair);
        this.iMapOpenLoot.set(getBackupKey(h), hashMap);
      } catch (Throwable e) {
        e.printStackTrace(System.out);
      }

      this.queueWrapper.setMyLid(lid);
      this.writeStealBackupVictim(loot.size());

      if (thief >= 0) {
        crashPlace(5, 2, 0, t, lid);
        crashPlace(11, 2, 0, t, lid);
        this.logger.lifelineStealsSuffered++;
        try {
          uncountedAsyncAt(
              place(thief),
              () -> {
                this.consolePrinter.println("Trying to enter this.waiting");
                synchronized (this.waiting) {
                  this.consolePrinter.println("Entered this.waiting");
                  this.deal(loot, victim, lid, true);
                  this.waiting.set(false);
                  this.waiting.notifyAll();
                }
                this.consolePrinter.println("Left this.waiting");
              });
        } catch (Throwable throwable) {
          throwable.printStackTrace(System.out);
        }
      } else {
        crashPlace(3, 2, 0, t, lid);
        if (t != 0) {
          crashPlace(6, 2, 0, t, lid);
        }
        if (t != 0) {
          crashPlace(7, 2, 0, t, lid);
        }
        this.logger.stealsSuffered++;
        try {
          uncountedAsyncAt(
              place(-thief - 1),
              () -> {
                this.consolePrinter.println("Trying to enter this.waiting");
                synchronized (this.waiting) {
                  this.consolePrinter.println("Entered this.waiting");
                  this.deal(loot, victim, lid, false);
                  waiting.set(false);
                  waiting.notifyAll();
                }
                this.consolePrinter.println("Left this.waiting");
              });
        } catch (Throwable throwable) {
          throwable.printStackTrace(System.out);
        }
        crashPlace(4, 2, 0, t, lid);
      }

    } else {
      this.logger.lifelineStealsSuffered++;

      final Integer thief = this.lifelineThieves.poll();
      if (thief == null) {
        System.out.println(
            here() + "(in give2): thief is null, should never happen)" + " lid " + lid);
        this.queue.mergeAtBottom(loot);
        return;
      }
      this.consolePrinter.println("lifeline thief=" + thief);

      final int t = thief < 0 ? -thief - 1 : thief;

      try {
        HashMap<Integer, Pair<Long, IncTaskBag>> hashMap = this.iMapOpenLoot.get(getBackupKey(h));
        Pair<Long, IncTaskBag> pair = new Pair<>(lid, loot);
        if (hashMap.get(t) != null) {
          System.out.println(
              here()
                  + "(in give): want to write openLoot, but is not null, should never happen!!!");
        }
        hashMap.put(t, pair);
        this.iMapOpenLoot.set(getBackupKey(h), hashMap);
      } catch (Throwable e) {
        e.printStackTrace(System.out);
      }

      this.queueWrapper.setMyLid(lid);
      this.writeStealBackupVictim(loot.size());

      crashPlace(5, 2, 0, t, lid);
      crashPlace(11, 2, 0, t, lid);

      try {
        asyncAt(
            place(t),
            () -> {
              this.deal(loot, victim, lid, true);
            });
      } catch (Throwable throwable) {
        throwable.printStackTrace(System.out);
      }
    }
    this.consolePrinter.println("end loot.size=" + loot.size());
  }

  /**
   * Writes cyclic regular backups. Distribute works to (lifeline) thieves by calling the {@link
   * #give(IncTaskBag)}
   */
  private void distribute() {
    this.consolePrinter.println("Trying to enter this.lifelineThieves");
    synchronized (this.lifelineThieves) {
      this.consolePrinter.println("Entered this.lifelineThieves");
      if (this.thieves.size() + this.lifelineThieves.size() > 0) {
        this.logger.startStoppingTimeWithAutomaticEnd(IncFTLogger.COMMUNICATION);
      } else {
        if (this.currentK >= this.k) {
          this.writeBackup();
        }
      }

      IncTaskBag loot;
      while (((this.thieves.size() > 0) || (this.lifelineThieves.size() > 0))
          && (loot = this.queue.split()) != null) {
        this.give(loot);
      }
    }
    this.consolePrinter.println("Left this.lifelineThieves");
  }

  /**
   * Rejecting thieves when no task to share (or worth sharing). Note, never reject lifeline thief,
   * instead put it into the lifelineThieves stack,
   */
  private void reject() {
    this.consolePrinter.println("Trying to enter this.lifelineThieves");
    synchronized (this.lifelineThieves) {
      this.consolePrinter.println("Entered this.lifelineThieves");
      this.logger.startStoppingTimeWithAutomaticEnd(IncFTLogger.COMMUNICATION);

      while (this.thieves.size() > 0) {
        final int thief = this.thieves.poll();
        if (thief >= 0) {
          this.consolePrinter.println("lifeline thief=" + thief);
          this.lifelineThieves.add(thief);
          try {
            uncountedAsyncAt(
                place(thief),
                () -> {
                  this.consolePrinter.println("Trying to enter this.waiting");
                  synchronized (this.waiting) {
                    this.consolePrinter.println("Entered this.waiting");
                    int newId = logger.startStoppingTime(IncFTLogger.COMMUNICATION);
                    this.waiting.set(false);
                    this.waiting.notifyAll();
                    this.logger.endStoppingTime(newId);
                  }
                  this.consolePrinter.println("Left this.waiting");
                });
          } catch (Throwable throwable) {
            throwable.printStackTrace(System.out);
          }
        } else {
          try {
            this.consolePrinter.println("random thief=" + thief);
            uncountedAsyncAt(
                place(-thief - 1),
                () -> {
                  this.consolePrinter.println("Trying to enter this.waiting");
                  synchronized (this.waiting) {
                    this.consolePrinter.println("Entered this.waiting");
                    int newId = logger.startStoppingTime(IncFTLogger.COMMUNICATION);
                    this.waiting.set(false);
                    this.waiting.notifyAll();
                    this.logger.endStoppingTime(newId);
                  }
                  this.consolePrinter.println("Left this.waiting");
                });
          } catch (Throwable throwable) {
            throwable.printStackTrace(System.out);
          }
        }
      }
    }
    this.consolePrinter.println("Left this.lifelineThieves");
  }

  /**
   * Send out steal requests. It does following things: (1) Probes w random victims and send out
   * stealing requests by calling into {@link #request(int, boolean)} (2) If probing random victims
   * fails, resort to lifeline buddies In both case, it sends out the request and wait on the
   * thieves' response, which either comes from or (ii) {@link #give(IncTaskBag)}
   *
   * @return !empty.get();
   */
  public boolean steal() {
    this.consolePrinter.println("begin");

    if (this.startPlacesSize == 1) {
      this.consolePrinter.println("end startPlacesSize==1");
      return false;
    }

    this.logger.startStoppingTimeWithAutomaticEnd(IncFTLogger.COMMUNICATION);
    final int p = here().id;
    for (int i = 0; i < this.w && this.empty.get(); ++i) {
      this.logger.stealsAttempted++;
      this.waiting.set(true);
      this.logger.stopLive();

      if (this.m <= 0) {
        this.consolePrinter.println("end m<=0");
        return false;
      }

      int v = this.victims[this.random.nextInt(this.m)];

      int count = 0;
      while (isDead(place(v))) {
        v = this.victims[this.random.nextInt(this.m)];
        count++;
        if (count >= 4) {
          this.consolePrinter.println("end count>=4");
          return false;
        }
      }

      this.waitingPlace = v;

      try {
        uncountedAsyncAt(
            place(v),
            () -> {
              this.request(p, false);
            });

        this.consolePrinter.println("Trying to enter this.waiting random");
        synchronized (this.waiting) {
          this.consolePrinter.println("Entered this.waiting random");
          this.logger.startStoppingTimeWithAutomaticEnd(IncFTLogger.WAITING);
          while (this.waiting.get()) {
            try {
              this.waiting.wait();
            } catch (InterruptedException e) {
              System.out.println(here() + ": " + e);
            }
          }
        }
        this.consolePrinter.println("Left this.waiting random");
      } catch (Throwable throwable) {
        throwable.printStackTrace(System.out);
        this.waiting.set(false);
        this.waiting.notifyAll();
      }
      this.logger.startLive();
    }

    for (int i = 0;
        (i < this.lifelines.length) && this.empty.get() && (0 <= this.lifelines[i]);
        ++i) {
      final int lifeline = this.lifelines[i];
      if (this.lifelinesActivated[lifeline] == false) {
        this.logger.lifelineStealsAttempted++;
        this.lifelinesActivated[lifeline] = true;
        this.waiting.set(true);

        if (isDead(place(lifeline))) {
          continue;
        }

        this.waitingPlace = lifeline;

        try {
          uncountedAsyncAt(
              place(lifeline),
              () -> {
                this.request(p, true);
              });

          this.consolePrinter.println("Trying to enter this.waiting lifeline");
          synchronized (this.waiting) {
            this.consolePrinter.println("Entered this.waiting lifeline");
            this.logger.startStoppingTimeWithAutomaticEnd(IncFTLogger.WAITING);
            while (this.waiting.get()) {
              try {
                this.waiting.wait();
              } catch (InterruptedException e) {
                e.printStackTrace(System.out);
              }
            }
          }
          this.consolePrinter.println("Left this.waiting lifeline");
        } catch (Throwable throwable) {
          this.waiting.set(false);
          this.waiting.notifyAll();
          throwable.printStackTrace(System.out);
        }
        this.logger.startLive();
      }
    }
    this.consolePrinter.println("end");
    return !(this.empty.get());
  }

  /**
   * Remote thief sending requests to local LJR. When empty or waiting for more work, reject
   * non-lifeline thief right away. Note, never reject lifeline thief.
   *
   * @param thief place id of thief
   * @param lifeline if I am the lifeline buddy of the remote thief
   */
  public void request(int thief, boolean lifeline) {
    this.consolePrinter.println("begin thief=" + thief + " lifeline=" + lifeline);
    try {
      this.consolePrinter.println("Trying to enter this.lifelineThieves");
      synchronized (this.lifelineThieves) {
        this.consolePrinter.println("Entered this.lifelineThieves");
        int newId = logger.startStoppingTime(IncFTLogger.COMMUNICATION);

        if (lifeline) {
          this.logger.lifelineStealsReceived++;
        } else {
          this.logger.stealsReceived++;
        }
        if (this.empty.get() || this.waiting.get()) {
          if (lifeline == true) {
            this.lifelineThieves.remove(Integer.valueOf(thief));
            if (this.iMapOpenLoot.get(getBackupKey(here().id)).get(thief) == null) {
              this.lifelineThieves.add(thief);
              this.consolePrinter.println("Added to lifelineThieves!");
            } else {
              this.consolePrinter.println("Not added to lifelineThieves!");
            }
          }
          try {
            this.consolePrinter.println("rejecting");
            uncountedAsyncAt(
                place(thief),
                () -> {
                  this.consolePrinter.println("Trying to enter this.waiting rejecting");
                  synchronized (this.waiting) {
                    this.consolePrinter.println("Entered this.waiting rejecting");
                    int thiefNewId = logger.startStoppingTime(IncFTLogger.COMMUNICATION);
                    this.waiting.set(false);
                    this.waiting.notifyAll();
                    this.logger.endStoppingTime(thiefNewId);
                  }
                  this.consolePrinter.println("Left this.waiting rejecting");
                });
          } catch (Throwable throwable) {
            throwable.printStackTrace(System.out);
          }
        } else {

          lifeline |= this.lifelineThieves.remove(Integer.valueOf(thief));
          if (this.iMapOpenLoot.get(getBackupKey(here().id)).get(thief) == null) {
            this.consolePrinter.println("Adding!");
            if (lifeline) {
              this.thieves.offer(thief);
            } else {
              this.thieves.offer(-thief - 1);
            }
          } else {
            this.consolePrinter.println("Rejecting");
            try {
              uncountedAsyncAt(
                  place(thief),
                  () -> {
                    this.consolePrinter.println("Trying to enter this.waiting Rejecting");
                    synchronized (this.waiting) {
                      this.consolePrinter.println("Entered this.waiting Rejecting");
                      int thiefNewId = logger.startStoppingTime(IncFTLogger.COMMUNICATION);
                      this.waiting.set(false);
                      this.waiting.notifyAll();
                      this.logger.endStoppingTime(thiefNewId);
                    }
                    this.consolePrinter.println("Left this.waiting Rejecting");
                  });
            } catch (Throwable throwable) {
              throwable.printStackTrace(System.out);
            }
          }
        }

        this.logger.endStoppingTime(newId);
      }
      this.consolePrinter.println("Left this.lifelineThieves");
    } catch (Throwable e) {
      e.printStackTrace(System.out);
    }
    this.consolePrinter.println("end thief=" + thief + " lifeline=" + lifeline);
  }

  /**
   * Merge current FTWorker'timestamps taskbag with incoming task bag.
   *
   * @param loot task bag to merge
   * @param lifeline if it is from a lifeline buddy
   */
  private void processLoot(IncTaskBag loot, boolean lifeline, long lid, int source) {
    this.consolePrinter.println(
        "begin loot.size() = "
            + loot.size()
            + " lifeline="
            + lifeline
            + " source="
            + source
            + " lid="
            + lid);

    if (lifeline) {
      this.logger.lifelineStealsPerpetrated++;
      this.logger.lifelineNodesReceived += loot.size();
    } else {
      this.logger.stealsPerpetrated++;
      this.logger.nodesReceived += loot.size();
    }
    this.queue.mergeAtBottom(loot);
    this.queueWrapper.setReceivedLid(source, lid);
    this.empty.set(false);

    this.consolePrinter.println(
        "end loot.size() = "
            + loot.size()
            + " lifeline="
            + lifeline
            + " source="
            + source
            + " lid="
            + lid);
  }

  /**
   * Main process function of FTWorker. It does 4 things: (1) execute at most n tasks (2) respond to
   * stealing requests (3) when not worth sharing tasks, reject the stealing requests (4) when
   * running out of tasks, steal from others
   */
  private void processStack() {
    this.consolePrinter.println("begin");

    if (((here().id == 0)
        && (this.queue.count() == 1)
        && (this.workingPlaces.get(here().id) == true))
        == false) {
      this.putWorkingPlaces(here(), true);
    }

    this.logger.startStoppingTimeWithAutomaticEnd(IncFTLogger.PROCESSING);
    boolean cont;
    int size;
    do {
      boolean process = false;
      do {
        this.consolePrinter.println("Trying to enter this.waiting");
        synchronized (this.waiting) {
          this.consolePrinter.println("Entered this.waiting");
          this.logger.startStoppingTimeWithAutomaticEnd(IncFTLogger.PROCESSING);

          if (this.currentK == 0) { // begin of backup interval
            takeSnapshot();
          }

          process = true;
          for (int i = 0; i < n; ++i) {
            if (queue.size() == 0) {
              process = false;
              break;
            }
            this.queue.process();
            crashPlace(8, 2, 60_000, queue.count(), queue.size());
            if (snap.minS >= queue.size() - 1) { // new min snap
              takeSnapshot();
            }
          }

          if (this.queue.size() > 0) {
            this.currentK++;
          }

          crashPlace(1, 2, 0);
          crashPlace(10, 2, 0);

          if (here().id == 0 && crashNumber == 12) {
            for (int i = 1; i < 1 + startPlacesSize * 0.9 && i < startPlacesSize; ++i) {
              if (isDead(place(i))) {
                continue;
              }
              try {
                uncountedAsyncAt(
                    place(i),
                    () -> {
                      crashPlace(12, here().id, 0);
                    });
              } catch (Throwable throwable) {
              }
            }
          }

          this.distribute();

          this.logger.startStoppingTimeWithAutomaticEnd(IncFTLogger.PROCESSING);
        }
        this.consolePrinter.println("Left this.waiting");

        this.reject();
      } while (process);

      this.empty.set(true);

      this.reject();

      this.consolePrinter.println("Trying to enter this.waiting 2");
      synchronized (this.waiting) {
        this.consolePrinter.println("Entered this.waiting 2");
        cont = this.steal() || 0 < this.queue.size();
        this.logger.startStoppingTimeWithAutomaticEnd(IncFTLogger.PROCESSING);
        this.active.set(cont);
        size = this.queue.size();
      }
      this.consolePrinter.println("Left this.waiting 2");
    } while (cont);

    takeSnapshot();
    this.writeBackup();

    crashPlace(2, 2, 0);

    this.reject();
    this.logger.startStoppingTimeWithAutomaticEnd(IncFTLogger.IDLING);

    this.putWorkingPlaces(here(), false);

    this.consolePrinter.println("end");
  }

  private void putWorkingPlaces(final Place place, final boolean state) {
    this.consolePrinter.println("begin place=" + place + " state=" + state);

    if (here().id == 0) {
      if (isDead(place) == true) {
        this.workingPlaces.put(place.id, false);
        return;
      }
      this.workingPlaces.put(place.id, (state && (isDead(place) == false)));
    } else {
      this.consolePrinter.println("Trying to enter this.waitingForPutWorking");
      synchronized (this.waitingForPutWorking) {
        this.consolePrinter.println("Entered this.waitingForPutWorking");
        while (this.waitingForPutWorking.get() == true) {
          try {
            this.waitingForPutWorking.wait();
          } catch (InterruptedException e) {
            System.out.println(here() + ": " + e);
          }
        }
      }
      this.consolePrinter.println("Left this.waitingForPutWorking");
      this.waitingForPutWorking.set(true);

      try {
        uncountedAsyncAt(
            place(0),
            () -> {
              if (isDead(place) == true) {
                this.workingPlaces.put(place.id, false);
                return;
              }
              this.workingPlaces.put(place.id, (state && (isDead(place) == false)));

              try {
                uncountedAsyncAt(
                    place,
                    () -> {
                      this.consolePrinter.println("Trying to enter this.waitingForPutWorking");
                      synchronized (this.waitingForPutWorking) {
                        this.consolePrinter.println("Entered this.waitingForPutWorking");
                        this.waitingForPutWorking.set(false);
                        this.waitingForPutWorking.notifyAll();
                      }
                      this.consolePrinter.println("Left this.waitingForPutWorking");
                    });
              } catch (Throwable t) {
                t.printStackTrace(System.out);
              }
            });
      } catch (Throwable throwable) {
        throwable.printStackTrace(System.out);
      }
    }
    this.consolePrinter.println("end place=" + place + " state=" + state);
  }

  /**
   * Entry point when workload is only known dynamically . The workflow is terminated when (1) No
   * one has work to do (2) Lifeline steals are responded
   *
   * @param start init method used in {@link IncFTWorker}, note the workload is not allocated,
   *     because the workload can only be self-generated.
   */
  public void main(Runnable start) {
    this.consolePrinter.println("begin");
    workerStartTime = System.currentTimeMillis();
    try {
      finish(
          () -> {
            try {
              this.restartDaemon();
              this.empty.set(false);
              this.active.set(true);
              this.logger.startLive();
              this.consolePrinter.println("start.run() invoke");
              start.run();
              this.consolePrinter.println("start.run() returned");
              this.processStack();
              this.logger.endStoppingTimeWithAutomaticEnd();
              this.logger.stopLive();
              this.logger.nodesCount = queue.count();
            } catch (Throwable t) {
              t.printStackTrace(System.out);
            }
          });
    } catch (DeadPlacesException e) {
      System.out.println(here() + "(in main1): DeadPlacesException caught");
      e.printStackTrace(System.out);
    }
    this.consolePrinter.println("end");
  }

  /**
   * Only started on place 0. Last running activity. Is responsible for restart places in all
   * surrounding finish for correct termination.
   */
  private void restartDaemon() {
    this.consolePrinter.println("begin");

    final int h = here().id;
    this.workingPlaces.put(h, true);
    final int startPlacesSize = places().size();

    async(
        () -> {
          this.consolePrinter.println("restartDaemon begin async");
          int countOpenLootLeft = 0;
          boolean cont = true;
          while (cont) {

            synchronized (this.workingPlaces) {
              this.workingPlaces.wait(100);
              cont = this.workingPlaces.containsValue(true);
            }

            while (this.restartPlaces.isEmpty() == false) {
              Integer i = this.restartPlaces.poll();
              if (i == null) {
                continue;
              }
              this.consolePrinter.println("Restarting place " + i);
              try {
                asyncAt(place(i), this::processStack);
              } catch (Throwable throwable) {
                this.consolePrinter.println("Throwable when restarting place " + i);
                throwable.printStackTrace(System.out);
              }
            }

            if (cont == true) {
              continue;
            }

            boolean openHandlerLeft = false;
            String openHandlerLeftOut = here() + "(in restartDaemon): openHandlerLeft: ";
            for (Map.Entry<Integer, HashMap<Integer, Boolean>> entry :
                this.countHandler.entrySet()) {
              int crashNum = entry.getKey();
              HashMap<Integer, Boolean> value = entry.getValue();
              openHandlerLeftOut += "crashNum " + crashNum + ": ";
              for (Map.Entry<Integer, Boolean> e : value.entrySet()) {
                if (e.getValue() == false) {
                  openHandlerLeft = true;
                  openHandlerLeftOut += e.getKey() + ", ";
                }
              }
            }

            cont |= !this.restartPlaces.isEmpty();
            cont |= openHandlerLeft;
            cont |= (this.countHandler.size() + places().size()) != startPlacesSize;

            if (cont == true) {
              continue;
            }

            for (Map.Entry<Integer, HashMap<Integer, Pair<Long, IncTaskBag>>> iMapOpenLootEntry :
                this.iMapOpenLoot.entrySet()) {
              for (Map.Entry<Integer, Pair<Long, IncTaskBag>> pairEntry :
                  iMapOpenLootEntry.getValue().entrySet()) {
                if (pairEntry.getValue() != null) {
                  countOpenLootLeft++;
                  openHandlerLeft = true;
                  cont = true;
                  break;
                }
              }
              if (openHandlerLeft == true) {
                break;
              }
            }
          }
          this.consolePrinter.println("restartDaemon end async");
        });
    this.consolePrinter.println("end");
  }

  /**
   * Entry point when workload can be known statically. The workflow is terminated when (1) No one
   * has work to do (2) Lifeline steals are responded
   */
  public void main() {
    workerStartTime = System.currentTimeMillis();
    try {
      this.empty.set(false);
      this.active.set(true);
      this.logger.startLive();
      this.processStack();
      this.logger.endStoppingTimeWithAutomaticEnd();
      this.logger.stopLive();
      this.logger.nodesCount = queue.count();
    } catch (Throwable t) {
      t.printStackTrace(System.out);
    }
  }

  /**
   * Deal workload to the theif. If the thief is active already, simply merge the taskbag. If the
   * thief is inactive, the thief gets reactiveated again.
   *
   * @param loot Task to share
   * @param source victim id
   * @param lid loot identifiers
   * @param lifeline is lifeline deal?
   */
  private void deal(IncTaskBag loot, int source, long lid, boolean lifeline) {
    this.consolePrinter.println(
        "begin loot.size()="
            + loot.size()
            + " source="
            + source
            + " lid="
            + lid
            + " lifeline="
            + lifeline);

    this.logger.startStoppingTimeWithAutomaticEnd(IncFTLogger.COMMUNICATION);

    final int h = here().id;
    final int s = source;

    boolean oldActive;

    this.consolePrinter.println("Trying to enter this.waiting");
    synchronized (this.waiting) {
      this.consolePrinter.println("Entered this.waiting");
      if (this.queueWrapper.getReceivedLid(s) >= lid
          || this.iMapBackup.get(getBackupKey(s)).getMyLid() < lid) {
        this.consolePrinter.println(
            "lid = " + lid + " receivedLid(s) >= lid or source.myLid < lid");
        return;
      }

      if (lifeline) {
        this.lifelinesActivated[source] = false;
      }

      oldActive = this.active.getAndSet(true);

      if (oldActive) {
        this.processLoot(loot, lifeline, lid, source);
      } else {
        this.logger.startLive();
        this.processLoot(loot, lifeline, lid, source);
      }

      this.writeStealBackupThief(loot.size(), source);

      crashPlace(9, 2, 0);

      try {
        final int thiefID = here().id;
        final Place thief = here();
        final long lidToDelete = lid;
        this.consolePrinter.println("Sending loot received to place " + s);
        uncountedAsyncAt(
            place(s),
            () -> {
              this.consolePrinter.println("Trying to enter this.waiting");
              synchronized (this.waiting) {
                this.consolePrinter.println("Entered this.waiting");
                boolean found = false;
                HashMap<Integer, Pair<Long, IncTaskBag>> integerPairHashMap =
                    this.iMapOpenLoot.get(getBackupKey(here().id));
                Pair<Long, IncTaskBag> pair = integerPairHashMap.get(thiefID);
                if (pair != null && pair.getFirst() == lidToDelete) {
                  integerPairHashMap.put(thiefID, null);
                  this.iMapOpenLoot.set(getBackupKey(here().id), integerPairHashMap);
                  found = true;
                  this.consolePrinter.println("Loot received msg received from thief " + thief.id);
                }
              }
              this.consolePrinter.println("Left this.waiting");
            });
      } catch (Throwable throwable) {
        throwable.printStackTrace(System.out);
      }
    }
    this.consolePrinter.println("Left this.waiting");

    this.consolePrinter.println("lid = " + lid + " oldActive = " + oldActive);
    if (oldActive == false) {
      this.processStack();
      this.logger.endStoppingTimeWithAutomaticEnd();
      this.logger.stopLive();
      this.logger.nodesCount = queue.count();
    }
    this.consolePrinter.println(
        "end loot.size()="
            + loot.size()
            + " source="
            + source
            + " lid="
            + lid
            + " lifeline="
            + lifeline);
  }

  /**
   * Deal workload to the thief. If the thief is active already, simply merge the taskbag. Only
   * called in placeFailureHandler
   *
   * @param loot task to share
   * @param source victim id
   * @param lid lid of loot
   * @return oldactive
   */
  private boolean dealCalledFromHandler(IncTaskBag loot, int source, long lid) {
    this.consolePrinter.println(
        "begin loot.size()=" + loot.size() + " source=" + source + " lid=" + lid);

    final int s = source;

    if (here().id != 0 && source == 2) {
      crashPlace(6, here().id, 0, source, lid, loot.size());
    }
    if (here().id != 0 && source == 2) {
      crashPlace(7, here().id, 0, source, lid, loot.size());
    }

    boolean oldActive = false;
    this.consolePrinter.println("Trying to enter this.waiting");
    synchronized (this.waiting) {
      this.consolePrinter.println("Entered this.waiting");
      if (this.queueWrapper.getReceivedLid(s) >= lid
          || this.iMapBackup.get(getBackupKey(s)).getMyLid() < lid) {
        this.consolePrinter.println(
            "lid = " + lid + " receivedLid(s) >= lid or source.myLid < lid");
        return true;
      }

      oldActive = this.active.getAndSet(true);
      this.consolePrinter.println("lid = " + lid + " oldActive = " + oldActive);

      if (oldActive) {
        this.processLoot(loot, false, lid, source);
      } else {
        this.logger.startLive();
        this.processLoot(loot, false, lid, source);
      }

      this.writeStealBackupThief(loot.size(), source);
    }
    this.consolePrinter.println("Left this.waiting");
    this.consolePrinter.println(
        "end loot.size()=" + loot.size() + " source=" + source + " lid=" + lid);
    return oldActive;
  }

  private void writeStealBackupVictim(long nStolen) {
    this.consolePrinter.println("Trying to enter this.waiting");
    synchronized (this.waiting) {
      this.logger.startWriteBackup();
      this.consolePrinter.println("Entered this.waiting");
      this.logger.startStoppingTimeWithAutomaticEnd(IncFTLogger.COMMUNICATION);
      try {
        this.consolePrinter.println(
            "Before: minS=" + this.snap.minS + ", lastSnapTasks=" + this.lastSnapTasks);
        final IncTaskBag tasksToBackup;
        final long tasksToRemove;
        final IncFTGLBResult<T> resultToBackup;
        final long newMyLid = queueWrapper.getMyLid();

        final long count;

        final long lastStableTasks = Math.max(0, lastSnapTasks - 1);
        final int caseID;
        if (nStolen <= lastStableTasks && nStolen <= snap.minS) { // case 1
          tasksToRemove = nStolen;
          tasksToBackup = null;
          snap.minS -= nStolen;
          lastSnapTasks -= nStolen;
          resultToBackup = null;
          count = 0;
          caseID = 1;
        } else if (nStolen > lastStableTasks && nStolen <= snap.minS) { // case 2
          // Backup is completely stolen
          // => Remove all tasks from backup
          tasksToRemove = lastSnapTasks;
          // merge on top tasks of the snapshot that survived the steal
          final long taskDiff = snap.minS - nStolen;
          tasksToBackup = queue.getFromBottom(taskDiff, 0);
          tasksToBackup.mergeAtTop(snap.taskA);
          snap.minS -= nStolen;
          lastSnapTasks = snap.minS + snap.taskA.size();
          resultToBackup = snap.result;
          count = snap.count;
          caseID = 2;
        } else { // case 3 and 4
          tasksToRemove = lastSnapTasks;
          takeSnapshot();
          lastSnapTasks = snap.minS + snap.taskA.size();
          tasksToBackup = queue.getFromBottom(queue.size(), 0);
          resultToBackup = snap.result;
          this.currentK = 0;
          count = snap.count;
          caseID = 3;
        }

        final long lastSnapTasks = this.lastSnapTasks;
        final long currentSnapTasks = this.snap.minS + this.snap.taskA.size();

        this.exactlyOnceExecutor.executeOnKey(
            this.iMapBackup,
            getBackupKey(here().id),
            new EntryProcessor() {
              @Override
              public Object process(Map.Entry entry) {
                consolePrinter.remotePrintln((Integer) entry.getKey(), "writeStealBackupVictim");
                final IncQueueWrapper<Queue, T> backupQueueWrapper =
                    (IncQueueWrapper<Queue, T>) entry.getValue();
                if (backupQueueWrapper == null) {
                  System.out.println(
                      "backupQueueWrapper == null. Should only happen on cluster shutdown");
                  return null;
                }
                final IncFTTaskQueue<Queue, T> backupQueue = backupQueueWrapper.queue;
                boolean done = backupQueueWrapper.getDone();
                if (done == false) {
                  consolePrinter.remotePrintln(
                      (Integer) entry.getKey(),
                      "before writeStealBackupVictim case="
                          + caseID
                          + " #backupTasks="
                          + backupQueue.size()
                          + " ("
                          + currentSnapTasks
                          + " ; "
                          + lastSnapTasks
                          + ")");
                  backupQueueWrapper.setMyLid(newMyLid);
                  if (resultToBackup != null) {
                    backupQueue.setResult(resultToBackup);
                  }
                  if (count != 0) {
                    backupQueue.setCount(count);
                  }

                  if (tasksToRemove > 0) {
                    backupQueue.removeFromBottom(tasksToRemove);
                  }
                  if (tasksToBackup != null) {
                    backupQueue.mergeAtBottom(tasksToBackup);
                  }
                  consolePrinter.remotePrintln(
                      (Integer) entry.getKey(),
                      "after writeStealBackupVictim case="
                          + caseID
                          + " #backupTasks="
                          + backupQueue.size()
                          + " ("
                          + currentSnapTasks
                          + " ; "
                          + lastSnapTasks
                          + ")");
                  entry.setValue(backupQueueWrapper);
                }
                return null;
              }

              @Override
              public EntryBackupProcessor getBackupProcessor() {
                return this::process;
              }
            });
        this.consolePrinter.println(
            "After: minS=" + this.snap.minS + ", lastSnapTasks=" + this.lastSnapTasks);
        if (caseID == 1) {
          crashPlace(13, 2, 0);
        }
        if (caseID == 2) {
          crashPlace(14, 2, 0);
        }
        if (caseID == 3) {
          crashPlace(15, 2, 0);
        }
      } catch (Throwable throwable) {
        this.consolePrinter.println("Throwable");
        throwable.printStackTrace(System.out);
        this.iMapBackup.set(getBackupKey(here().id), this.queueWrapper);
        takeSnapshot();
        this.lastSnapTasks = this.snap.minS + this.snap.taskA.size();
      }
      this.logger.stealBackupsWritten++;
      this.logger.stopWriteBackup();
    }
    this.consolePrinter.println("Left this.waiting");
  }

  private void writeStealBackupThief(long nStolen, int source) {
    this.consolePrinter.println("Trying to enter this.waiting");
    boolean emptyPoolSituation = false;
    synchronized (this.waiting) {
      this.logger.startWriteBackup();
      this.consolePrinter.println("Entered this.waiting");
      this.logger.startStoppingTimeWithAutomaticEnd(IncFTLogger.COMMUNICATION);
      try {
        final IncTaskBag tasksToBackup = queue.getFromBottom(nStolen, 0);
        final long lid = queueWrapper.getReceivedLid(source);

        this.consolePrinter.println(
            "Before: minS=" + this.snap.minS + ", lastSnapTasks=" + this.lastSnapTasks);

        snap.minS += nStolen;
        lastSnapTasks += nStolen;

        final IncFTGLBResult<T> emptyPoolResult;
        final long count = this.queue.count();
        if (this.queue.size() - nStolen == 0) { // pool was empty before merge of loot
          takeSnapshot();
          emptyPoolResult = this.snap.result;
          emptyPoolSituation = true;
          this.currentK = 0;
          this.lastSnapTasks = snap.minS + snap.taskA.size();
        } else {
          emptyPoolResult = null;
          if (snap.taskA.size() == 0) {
            --snap.minS; // subtract task A from stable tasks
            snap.taskA = queue.getFromBottom(1, snap.minS);
          }
        }

        final long lastSnapTasks = this.lastSnapTasks;
        final long currentSnapTasks = this.snap.minS + this.snap.taskA.size();

        this.exactlyOnceExecutor.executeOnKey(
            this.iMapBackup,
            getBackupKey(here().id),
            new EntryProcessor() {
              @Override
              public Object process(Map.Entry entry) {
                consolePrinter.remotePrintln((Integer) entry.getKey(), "writeStealBackupThief");
                IncQueueWrapper<Queue, T> backupQueueWrapper =
                    (IncQueueWrapper<Queue, T>) entry.getValue();
                if (backupQueueWrapper == null) {
                  System.out.println(
                      "backupQueueWrapper == null. Should only happen on cluster shutdown");
                  return null;
                }
                final IncFTTaskQueue<Queue, T> backupQueue = backupQueueWrapper.queue;
                boolean done = backupQueueWrapper.getDone();
                if (done == false) {
                  consolePrinter.remotePrintln(
                      (Integer) entry.getKey(),
                      "before writeStealBackupThief #backupTasks="
                          + backupQueue.size()
                          + " ("
                          + currentSnapTasks
                          + " ; "
                          + lastSnapTasks
                          + ")");

                  if (emptyPoolResult != null) {
                    backupQueue.removeFromBottom(backupQueue.size());
                    backupQueue.setResult(emptyPoolResult);
                    backupQueue.setCount(count);
                  }

                  backupQueueWrapper.setReceivedLid(source, lid);
                  backupQueue.mergeAtBottom(tasksToBackup);

                  consolePrinter.remotePrintln(
                      (Integer) entry.getKey(),
                      "after writeStealBackupThief #backupTasks="
                          + backupQueue.size()
                          + " ("
                          + currentSnapTasks
                          + " ; "
                          + lastSnapTasks
                          + ")");

                  entry.setValue(backupQueueWrapper);
                }
                return null;
              }

              @Override
              public EntryBackupProcessor getBackupProcessor() {
                return this::process;
              }
            });
        this.consolePrinter.println(
            "After: minS=" + this.snap.minS + ", lastSnapTasks=" + this.lastSnapTasks);
      } catch (Throwable throwable) {
        this.consolePrinter.println("Throwable");
        throwable.printStackTrace(System.out);
        this.iMapBackup.set(getBackupKey(here().id), this.queueWrapper);
        takeSnapshot();
        this.lastSnapTasks = this.snap.minS + this.snap.taskA.size();
      }
      this.logger.stealBackupsWritten++;
      this.logger.stopWriteBackup();
    }
    this.consolePrinter.println("Left this.waiting");
    if (emptyPoolSituation == false) {
      crashPlace(16, 2, 0);
    } else {
      crashPlace(18, 2, 0);
    }
  }

  /** Merges the min-snapshot into iMapBackup. */
  private void writeBackup() {
    this.consolePrinter.println("Trying to enter this.waiting");
    synchronized (this.waiting) {
      this.logger.startWriteBackup();
      this.consolePrinter.println("Entered this.waiting");
      this.logger.startStoppingTimeWithAutomaticEnd(IncFTLogger.COMMUNICATION);
      try {
        this.consolePrinter.println(
            "Before: minS=" + this.snap.minS + ", lastSnapTasks=" + this.lastSnapTasks);

        final Snapshot<T> snap = this.snap;
        final long taskDiff = snap.minS - Math.max(0, this.lastSnapTasks - 1);

        final IncTaskBag tasksToBackup;
        if (taskDiff > 0) {
          tasksToBackup = queue.getFromBottom(taskDiff, Math.max(0, this.lastSnapTasks - 1));
        } else {
          tasksToBackup = null;
        }

        this.currentK = 0;
        this.lastSnapTasks = snap.minS + snap.taskA.size();

        final long lastSnapTasks = this.lastSnapTasks;
        final long currentSnapTasks = this.snap.minS + this.snap.taskA.size();
        final long count = this.snap.count;
        final IncFTGLBResult<T> result = this.snap.result;
        final IncTaskBag taskA = this.snap.taskA;

        this.exactlyOnceExecutor.executeOnKey(
            this.iMapBackup,
            getBackupKey(here().id),
            new EntryProcessor() {
              @Override
              public Object process(Map.Entry entry) {
                consolePrinter.remotePrintln((Integer) entry.getKey(), "writeBackup");
                IncQueueWrapper<Queue, T> backupQueueWrapper =
                    (IncQueueWrapper<Queue, T>) entry.getValue();
                if (backupQueueWrapper == null) {
                  System.out.println(
                      "backupQueueWrapper == null. Should only happen on cluster shutdown");
                  return null;
                }
                final IncFTTaskQueue<Queue, T> backupQueue = backupQueueWrapper.queue;
                boolean done = backupQueueWrapper.getDone();
                if (done == false) {
                  consolePrinter.remotePrintln(
                      (Integer) entry.getKey(),
                      "before writeBackup #backupTasks="
                          + backupQueue.size()
                          + " ("
                          + currentSnapTasks
                          + " ; "
                          + lastSnapTasks
                          + ")");
                  backupQueue.setResult(result);
                  backupQueue.setCount(count);
                  if (backupQueue.size() > 0) {
                    backupQueue.removeFromTop(1); // remove old A
                  }
                  if (taskDiff > 0) {
                    backupQueue.mergeAtTop(tasksToBackup); // new stable tasks
                  } else if (taskDiff < 0) {
                    backupQueue.removeFromTop(-taskDiff); // remove done tasks
                  }
                  if (snap.taskA.size() > 0) {
                    backupQueue.mergeAtTop(taskA); // merge new A
                  }

                  consolePrinter.remotePrintln(
                      (Integer) entry.getKey(),
                      "after writeBackup #backupTasks="
                          + backupQueue.size()
                          + " ("
                          + currentSnapTasks
                          + " ; "
                          + lastSnapTasks
                          + ")");

                  entry.setValue(backupQueueWrapper);
                } else {
                  consolePrinter.remotePrintln((Integer) entry.getKey(), "done == true");
                }
                return null;
              }

              @Override
              public EntryBackupProcessor getBackupProcessor() {
                return this::process;
              }
            });
        this.consolePrinter.println(
            "After: minS=" + this.snap.minS + ", lastSnapTasks=" + this.lastSnapTasks);
        crashPlace(17, 2, 0);
      } catch (Throwable throwable) {
        this.consolePrinter.println("Throwable");
        throwable.printStackTrace(System.out);
        this.iMapBackup.set(getBackupKey(here().id), this.queueWrapper);
        takeSnapshot();
        this.lastSnapTasks = this.snap.minS + this.snap.taskA.size();
      }
      this.logger.regularBackupsWritten++;
      this.logger.stopWriteBackup();
    }
    this.consolePrinter.println("Left this.waiting");
  }

  /** Kills all alive places and removes the partitionLostListeners */
  private void killAllPlaces() {
    this.consolePrinter.println("begin");
    this.iMapBackup.removePartitionLostListener(this.iMapBackupHandlerRemoveID);
    this.iMapOpenLoot.removePartitionLostListener(this.iMapOpenLootHandlerRemoveID);
    GlobalRuntime.getRuntime().setPlaceFailureHandler((deadPlace) -> System.exit(40));
    GlobalRuntime.getRuntime().setRuntimeShutdownHandler(null);

    this.consolePrinter.println("before finish");
    finish(
        () -> {
          for (Place place : places()) {
            if (place.id == here().id) {
              continue;
            }
            try {
              asyncAt(
                  place,
                  () -> {
                    this.iMapBackup.removePartitionLostListener(this.iMapBackupHandlerRemoveID);
                    this.iMapOpenLoot.removePartitionLostListener(this.iMapOpenLootHandlerRemoveID);
                    GlobalRuntime.getRuntime()
                        .setPlaceFailureHandler((deadPlace) -> System.exit(44));
                    GlobalRuntime.getRuntime().setRuntimeShutdownHandler(null);
                    System.exit(44);
                  });
            } catch (Throwable throwable) {
            }
          }
        });
    this.consolePrinter.println("after finish");
    System.exit(44);
  }

  private void crashPlace(int crashNumber, int place, long millisAfterStart, long... opts) {
    if (crashNumber != this.crashNumber) {
      return;
    }
    if (here().id == place && System.currentTimeMillis() >= (workerStartTime + millisAfterStart)) {
      String callerName = Thread.currentThread().getStackTrace()[2].getMethodName();
      System.out.println(
          here()
              + " (in "
              + callerName
              + "): shutdown, case "
              + crashNumber
              + ", opts: "
              + Arrays.toString(opts));
      System.exit(crashNumber);
    }
  }

  /** Used for incremental backups */
  private static class Snapshot<T extends Serializable> implements Serializable {

    int minS;
    IncTaskBag taskA;
    IncFTGLBResult<T> result;
    long count;
  }
}
