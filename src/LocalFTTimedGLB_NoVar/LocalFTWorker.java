/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package LocalFTTimedGLB_NoVar;

import static apgas.Constructs.*;

import FTGLB.FTLogger;
import apgas.GlobalRuntime;
import apgas.Place;
import apgas.SerializableCallable;
import apgas.util.PlaceLocalObject;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalList;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTaskContext;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import utils.ConsolePrinter;
import utils.ReadOnlyEntryProcessor;

/**
 * The local runner for the Cooperative.FTGLB framework. An instance of this class runs at each
 * place and provides the context within which user-specified tasks execute and are load balanced
 * across all places.
 *
 * @param <Queue> Concrete TaskQueue type
 * @param <T> Result type.
 */
public final class LocalFTWorker<Queue extends LocalFTTaskQueue<Queue, T>, T>
    extends PlaceLocalObject implements Serializable {
  private static final long serialVersionUID = 1L;

  /** the resilient map for backups */
  protected final transient IMap<Integer, LocalFTTaskBag> iMapBags;

  protected final transient IMap<Long, ArrayList<LocalFTTaskBagSplit>> iMapSplits;

  protected final transient IList<Integer> replayList;

  protected final String iMapBagsHandlerRemoveID;
  protected final String iMapSplitsHandlerRemoveID;

  /** hazelcast instance */
  private final transient HazelcastInstance hz = Hazelcast.getHazelcastInstanceByName("apgas");

  /** number of places at start */
  private final int startPlacesSize;

  /**
   * Random number, used when picking a non-lifeline victim/buddy. Important to seed with place id,
   * otherwise BG/Q, the random sequence will be exactly same at different places
   */
  private final Random random = new Random(here().id);

  /** states of worker */
  private final transient AtomicBoolean active = new AtomicBoolean(false);

  private final transient AtomicBoolean empty = new AtomicBoolean(true);

  private final transient AtomicBoolean waiting = new AtomicBoolean(false);
  private final Lock waitingLock = new ReentrantLock(true);
  private final Condition waitingCondition = waitingLock.newCondition();

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

  /** TaskQueue, responsible for crunching numbers */
  Queue queue;

  private final HashMap<Integer, Integer> placeKeyMap;

  /** Logger to record the work-stealing status */
  FTLogger logger;

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

  /** Thieves that send stealing requests */
  private ArrayList<Integer> deadPlaces;
  //    private final IAtomicLong iCurrentLid;

  /** Waiting for response of this place */
  private int waitingPlace;

  /** Name of current iMapBackup for backups */
  private String bagsMapName;

  private String splitsMapName;

  private String replayListName;

  private int crashNumber;
  private long workerStartTime = -1;

  private LocalFTTaskBag currentBag;
  private long currentStep;
  public long currentSplit;
  public ArrayList<Long> lastBagIds;
  public final ArrayList<LocalFTTaskBagSplit> currentSplits;
  private long currentBackupId;
  private final HashSet<Long> ownedBags;

  private long s;
  /** Timestamp of last written backup */
  private long timestampLastBackup;

  private LinkedList<ICompletableFuture<Void>> runningBackups;
  private AtomicBoolean blockBackups;
  private AtomicBoolean waitingForLifeline;
  private AtomicInteger waitingForLifelinePlace;

  private ConcurrentHashMap<Integer, LocalFTGLBResult> resultMap;

  public ConsolePrinter consolePrinter;

  private ArrayList<LocalFTTaskBagSplit> currentReplaySplits;

  private int currentDescriptionRecord;

  private final ArrayList<Integer> descriptionRecords;

  private int getIMapKey(int placeID) {
    if (placeID > 2 * startPlacesSize) return placeID + 1;
    return this.placeKeyMap.get((placeID + 1) % startPlacesSize);
  }

  /**
   * Class constructor
   *
   * @param init function closure to init the local {@link LocalFTWorker}
   * @param n same to this.n
   * @param w same to this.w
   * @param l power of lifeline graph
   * @param z base of lifeline graph
   * @param m same to this.m
   * @param tree true if the workload is dynamically generatedf, false if the workload can be
   *     statically generated
   * @param s true if stopping Time in Logger, false if not
   * @param placeKeyMap
   */
  LocalFTWorker(
      SerializableCallable<Queue> init,
      int n,
      int w,
      int l,
      int z,
      int m,
      boolean tree,
      int timestamps,
      int crashNumber,
      int backupCount,
      int P,
      HashMap<Integer, Integer> placeKeyMap,
      long s) {
    this.startPlacesSize = P;
    this.placeKeyMap = placeKeyMap;

    this.n = n;
    this.w = w;
    this.m = m;
    this.waitingForPutWorking = new AtomicBoolean(false);

    this.crashNumber = crashNumber;
    this.currentStep = 0L;
    this.currentSplit = 0L;
    this.waitingPlace = -1;
    this.runningBackups = new LinkedList<>();
    this.blockBackups = new AtomicBoolean(false);
    this.lastBagIds = new ArrayList<>();
    this.waitingForLifeline = new AtomicBoolean(false);
    this.waitingForLifelinePlace = new AtomicInteger(0);
    this.ownedBags = new HashSet<>();
    this.resultMap = new ConcurrentHashMap<>();
    this.descriptionRecords = new ArrayList<>();

    this.consolePrinter = new ConsolePrinter();

    this.s = s;
    // this.consolePrinter = ConsolePrinter.getInstance();

    // only used on place 0
    this.workingPlaces = new HashMap<>();
    this.restartPlaces = new ConcurrentLinkedQueue<>();
    this.countHandler = new HashMap<>();
    if (here().id == 0) {
      for (int i = 0; i < this.startPlacesSize; i++) {
        this.workingPlaces.put(i, false);
      }
    }

    this.lifelines = new int[z];
    Arrays.fill(this.lifelines, -1);

    int h = here().id;

    this.victims = new int[m];
    if (startPlacesSize > 1) {
      for (int i = 0; i < m; i++) {
        while ((this.victims[i] = this.random.nextInt(startPlacesSize)) == h) {}
      }
    }

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

    try {
      this.queue = init.call();
    } catch (Throwable throwable) {
      throwable.printStackTrace(System.out);
    }
    this.logger = new FTLogger(timestamps);

    this.bagsMapName = "iMapBags";
    MapConfig backupMapConfig = new MapConfig(this.bagsMapName);
    backupMapConfig.setBackupCount(backupCount);
    backupMapConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
    this.hz.getConfig().addMapConfig(backupMapConfig);
    this.iMapBags = hz.getMap(this.bagsMapName);

    this.currentBag = queue.getAllTasks();
    this.iMapBags.set(getIMapKey(here().id), currentBag);
    this.logger.incrementBackupsWritten(FTLogger.INITBACKUP);
    this.timestampLastBackup = System.nanoTime();

    this.splitsMapName = "iMapSplits";
    MapConfig splitsMapConfig = new MapConfig(this.splitsMapName);
    splitsMapConfig.setBackupCount(backupCount);
    splitsMapConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
    this.hz.getConfig().addMapConfig(splitsMapConfig);
    this.iMapSplits = hz.getMap(this.splitsMapName);

    this.replayListName = "replayList";
    ListConfig replayListConfig = new ListConfig(this.replayListName);
    replayListConfig.setBackupCount(backupCount);
    replayList = this.hz.getList(replayListName);

    this.currentSplits = new ArrayList<>();

    this.iMapBagsHandlerRemoveID =
        this.iMapBags.addPartitionLostListener(
            event -> {
              System.out.println(
                  here()
                      + "(in partitionLostListener - iMapBags): "
                      + event.toString()
                      + " ...shutting down the cluster now, result will be wrong");
              this.killAllPlaces();
            });

    this.iMapSplitsHandlerRemoveID =
        this.iMapSplits.addPartitionLostListener(
            event -> {
              System.out.println(
                  here()
                      + "(in partitionLostListener - iMapSplits): "
                      + event.toString()
                      + " ...shutting down the cluster now, result will be wrong");
              this.killAllPlaces();
            });

    this.lifelineThieves = new LinkedList<>();

    this.thieves = new LinkedList<>();
    this.lifelinesActivated = new boolean[startPlacesSize];

    synchronized (this.lifelineThieves) {
      if (tree) {
        int[] calculateLifelineThieves = calculateLifelineThieves(l, z, h);
        for (int i : calculateLifelineThieves) {
          if (i != -1) {
            this.lifelineThieves.add(i);
            this.consolePrinter.println("Add " + i + " to lifelineThieves");
          }
        }
        for (int i : this.lifelines) {
          if (i != -1) {
            this.lifelinesActivated[i] = true;
            this.consolePrinter.println("Activate lifeline " + i);
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
  }

  /**
   * Internal method used by {@link LocalFTGLB} to start FTWorker at each place when the workload is
   * known statically.
   */
  static <Queue extends LocalFTTaskQueue<Queue, T>, T> void broadcast(
      LocalFTWorker<Queue, T> worker) {
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
    } catch (Throwable e) {
      System.out.println(here() + "(in broadcast): Throwable caught");
      e.printStackTrace(System.out);
    }
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
    this.consolePrinter.println("begin");
    try {
      if (this.crashNumber == 4) {
        try {
          Thread.sleep(120000);
        } catch (Throwable t) {
          t.printStackTrace();
        }
      }
      final Place placeFailureHandlerPlace = here();

      if (deadPlace.id == 0) {
        this.killAllPlaces();
        return;
      }

      this.consolePrinter.println("before this.waiting");
      this.waitingLock.lock();
      try {
        this.consolePrinter.println("after this.waiting");
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
        /*        final ArrayList<Integer> deadPlacesFromFirstHandler = this.deadPlaces;
        try {
          uncountedAsyncAt(
              place(0),
              () -> {
                synchronized (this.countHandler) {
                  if (this.countHandler.containsKey(deadPlace.id) == false) {
                    HashMap<Integer, Boolean> openHandlerList = new HashMap<>();
                    for (Place p : places()) {
                      openHandlerList.put(p.id, false);
                    }
                    for (int deadPlacesId : deadPlacesFromFirstHandler) {
                      openHandlerList.put(deadPlacesId, true);
                    }
                    this.countHandler.put(deadPlace.id, openHandlerList);
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
              });
        } catch (Throwable t) {
          t.printStackTrace(System.out);
        }*/

        this.consolePrinter.println("Before entering lifelinethieves");
        synchronized (this.lifelineThieves) {
          this.lifelineThieves.remove(Integer.valueOf(deadPlace.id));
          this.thieves.remove(Integer.valueOf(deadPlace.id));
          this.thieves.remove(Integer.valueOf(-deadPlace.id - 1));
          this.consolePrinter.println("Remove " + deadPlace.id + " from lifelineThieves");
          this.consolePrinter.println("Remove " + deadPlace.id + " from thieves");
          this.consolePrinter.println("Remove " + (-deadPlace.id - 1) + " from thieves");
        }

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
        this.consolePrinter.println("Deactivate lifeline " + deadPlace.id);

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

        this.m = tmpList.size();

        this.consolePrinter.println("Before this.runningBackups");
        synchronized (this.runningBackups) {
          this.consolePrinter.println("inside this.runningBackups!");
          this.logger.startWriteBackup();
          this.waitForBackups();
          while (this.blockBackups.get() == true) {
            try {
              this.runningBackups.wait();
            } catch (Throwable t) {
              t.printStackTrace();
            }
          }
          this.logger.stopWriteBackup();
          this.consolePrinter.println("after wait for backup!");

          // are we the backup place?
          Place currentDeadPlace = deadPlace;
          if (prevPlace(deadPlace).id == here().id) {
            this.consolePrinter.println("Before waiting for cluster safe");
            while (!this.hz.getPartitionService().isClusterSafe()) {
              try {
                Thread.sleep(100);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            }
            // we are the backup place
            while (isDead(currentDeadPlace)) {
              // restore all places until we reach an alive place
              restorePlace(currentDeadPlace);

              currentDeadPlace = place((currentDeadPlace.id + 1) % this.startPlacesSize);
            }
          }

          // did we get loot from deadPlace?
          this.iMapBags.lock(getIMapKey(here().id));
          try {
            final LocalFTTaskBag bagInIMap = this.iMapBags.get(getIMapKey(here().id));
            if (bagInIMap != null && bagInIMap.parentPlace == deadPlace.id) {
              if (!this.ownedBags.contains(bagInIMap.bagId)) {
                this.ownedBags.add(bagInIMap.bagId);
                boolean lifeline =
                    waitingForLifeline.get() && lifelinesActivated[deadPlace.id] == true;
                this.processLoot(bagInIMap, lifeline, deadPlace.id);
                boolean oldActive = this.active.getAndSet(true);
                if (!lifeline) {
                  this.consolePrinter.println("random loot found. stop waiting");
                  this.waiting.set(false);
                  this.waitingCondition.signalAll();
                } else {
                  this.consolePrinter.println("lifeline loot found");
                  this.lifelinesActivated[deadPlace.id] = false;
                  this.consolePrinter.println("Deactivate lifeline " + deadPlace.id);
                  this.waitingForLifeline.set(false);
                  this.consolePrinter.println("Set waitingForLifeline to false");
                  if (oldActive == false) {
                    this.consolePrinter.println("Restart me");
                    this.restartPlaceWithDaemon(here());
                  }
                }
              }
            }
          } finally {
            this.iMapBags.unlock(getIMapKey(here().id));
          }
        }
        if (this.waitingForLifeline.get() && this.waitingForLifelinePlace.get() == deadPlace.id) {
          this.waitingForLifeline.set(false);
        }

        if (this.waitingPlace == deadPlace.id) {
          this.consolePrinter.println("Stop waiting for deadPlace=" + deadPlace.id);
          this.waiting.set(false);
          this.waitingCondition.signalAll();
          this.waitingPlace = -1;
        }

        /*        if (here().id == 0) {
          // waiting for delayed refresh of countHandler
          while (this.countHandler.containsKey(deadPlace.id) == false) {
            try {
              Thread.sleep(500);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
          this.countHandler.get(deadPlace.id).put(here().id, true);
        } else {
          try {
            uncountedAsyncAt(
                place(0),
                () -> {
                  // waiting for delayed refresh of countHandler
                  while (this.countHandler.containsKey(deadPlace.id) == false) {
                    try {
                      Thread.sleep(500);
                    } catch (InterruptedException e) {
                      e.printStackTrace();
                    }
                  }
                  this.countHandler.get(deadPlace.id).put(placeFailureHandlerPlace.id, true);
                });
          } catch (Throwable throwable) {
            throwable.printStackTrace();
          }
        }*/
      } finally {
        this.waitingLock.unlock();
      }
    } catch (Throwable t) {
      t.printStackTrace();
    }
    this.consolePrinter.println("leave");
  }

  private void restoreWithReplayUnit(int deadPlace) {
    final LocalFTTaskBag bag = this.iMapBags.get(getIMapKey(deadPlace));
    ArrayList<LocalFTTaskBagSplit> splits = this.iMapSplits.get(bag.bagId);

    this.queue.merge(bag);
    this.empty.set(false);

    if (splits == null) splits = new ArrayList<>();

    this.currentReplaySplits = new ArrayList<>();
    this.currentBag = bag;
    this.currentDescriptionRecord = deadPlace;

    for (LocalFTTaskBagSplit split : splits) {
      if (split.backupId != bag.backupId) continue;
      this.currentReplaySplits.add(split);
    }

    this.consolePrinter.println("Set up environment for restore");
  }

  private void restorePlace(Place deadPlace) {
    try {
      crashPlace(2, 1, 0);

      final TransactionOptions txOptions =
          new TransactionOptions().setTimeout(15, TimeUnit.SECONDS);
      while (true) {
        try {
          this.hz.executeTransaction(
              txOptions,
              (TransactionalTaskContext context) -> {
                final TransactionalMap<Integer, LocalFTTaskBag> transBagsMap =
                    context.getMap(bagsMapName);
                final TransactionalList<Integer> transReplayList = context.getList(replayListName);
                LocalFTTaskBag lostBag = transBagsMap.getForUpdate(getIMapKey(deadPlace.id));
                if (lostBag != null && lostBag.recovered == false) {
                  if (lostBag.size() > 0) {
                    this.consolePrinter.println(
                        "Add description record bc size()=" + lostBag.size() + ">0");
                    transReplayList.add(deadPlace.id);
                    if (!this.descriptionRecords.contains(deadPlace.id)) {
                      this.descriptionRecords.add(deadPlace.id);
                      boolean oldActive = this.active.getAndSet(true);
                      if (!oldActive) {
                        restartPlaceWithDaemon(here());
                      }
                    }
                  }
                  lostBag.recovered = true;
                  transBagsMap.set(getIMapKey(deadPlace.id), lostBag);
                }
                return null;
              });
          break;
        } catch (Exception e) {
          e.printStackTrace();
        }
      }

      crashPlace(3, 1, 0);
    } catch (Throwable thr) {
      thr.printStackTrace();
    }
  }

  public void writeStealBackup(
      LocalFTTaskBag loot, ArrayList<LocalFTTaskBagSplit> splits, int thief, long parentBag) {
    boolean recovered = false;

    // this.consolePrinter.println("begin");

    try {
      if (here().id != 0 && crashNumber == 9 && thief == 2) {
        uncountedAsyncAt(
            place(thief),
            () -> {
              crashPlace(9, 2, 0);
            });
        Thread.sleep(30000);
      }
    } catch (Throwable t) {
      t.printStackTrace();
    }

    final LocalFTGLBResult lastResult = this.resultMap.remove(thief);

    synchronized (this.runningBackups) {
      this.waitForBackups();

      final ArrayList<Long> _lastBagIds = (ArrayList<Long>) this.lastBagIds.clone();
      uncountedAsyncAt(
          here(),
          () -> {
            for (Long lastBagId :
                _lastBagIds) { // cleanup orphan splits to prevent them from piling up
              this.iMapSplits.delete(lastBagId);
            }
          });
      this.lastBagIds.clear();

      final TransactionOptions txOptions =
          new TransactionOptions().setTimeout(15, TimeUnit.SECONDS);
      while (true) {
        try {
          recovered =
              this.hz.executeTransaction(
                  txOptions,
                  (TransactionalTaskContext context) -> {
                    final TransactionalMap<Integer, LocalFTTaskBag> transBagsMap =
                        context.getMap(bagsMapName);
                    final TransactionalMap<Long, ArrayList<LocalFTTaskBagSplit>> transSplitsMap =
                        context.getMap(splitsMapName);
                    final LocalFTTaskBag thiefBag;

                    thiefBag = transBagsMap.getForUpdate(getIMapKey(thief));

                    boolean alreadyRecovered = thiefBag != null && thiefBag.recovered;

                    if (thiefBag != null) {
                      if (lastResult != null) {
                        loot.ownerResult = lastResult;
                      } else {
                        loot.ownerResult = thiefBag.ownerResult;
                      }
                    }
                    loot.recovered = thiefBag.recovered;

                    transSplitsMap.set(parentBag, splits);

                    if (alreadyRecovered) {
                      final TransactionalList<Integer> transReplayList =
                          context.getList(replayListName);
                      final int pid = 2 * this.startPlacesSize + thief;
                      transBagsMap.set(getIMapKey(pid), loot);
                      transReplayList.add(pid);
                      this.consolePrinter.println(
                          "Add Description Record for stolen Tasks from thief=" + thief);
                      if (!this.descriptionRecords.contains(pid)) {
                        this.descriptionRecords.add(pid);
                      }
                    } else {
                      transBagsMap.set(getIMapKey(thief), loot);
                    }

                    return alreadyRecovered;
                  });

          this.logger.incrementBackupsWritten(FTLogger.STEALBACKUP);
          this.timestampLastBackup = System.nanoTime();

          break;
        } catch (Throwable t) {
          t.printStackTrace();
        }
      }
      this.blockBackups.set(false);
      this.runningBackups.notifyAll();
    }
    loot.ownerResult = null;
  }

  /**
   * Send out the workload to thieves. At this point, either thieves or lifeline thieves is
   * non-empty (or both are non-empty). Note sending workload to the lifeline thieve is the only
   * place that uses async (instead of uncounted async as in other places), which means when only
   * all lifeline requests are responded can the framework be terminated.
   *
   * @param loot the Taskbag(aka workload) to send out
   */
  public void give(final LocalFTTaskBag loot, int thief, boolean lifeline) {
    // this.consolePrinter.println("begin");
    final int victim = here().id;
    logger.nodesGiven += loot.size();

    final LocalFTTaskBagSplit split = new LocalFTTaskBagSplit();
    split.step = currentStep;
    split.split = currentSplit++;
    split.backupId = this.currentBackupId;

    synchronized (this.runningBackups) {
      while (this.blockBackups.get() == true) {
        try {
          this.runningBackups.wait();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      this.blockBackups.set(true);
      this.currentSplits.add(split);
    }

    final ArrayList<LocalFTTaskBagSplit> _currentSplits =
        (ArrayList<LocalFTTaskBagSplit>) this.currentSplits.clone();

    // this.consolePrinter.println("after this.runningBackups!");

    if (lifeline == false) {
      this.logger.stealsSuffered++;
      // this.consolePrinter.println("2giving " + loot.size() + " tasks to " + split.thief + "
      // bagId=" + loot.bagId);
      try {
        this.consolePrinter.println("Step 1 Give Random Loot to thief=" + thief);
        uncountedAsyncAt(
            here(),
            () -> {
              this.consolePrinter.println("Step 2 Give Random Loot to thief=" + thief);
              this.writeStealBackup(loot, _currentSplits, thief, currentBag.bagId);
              this.consolePrinter.println("Step 3 Give Random Loot to thief=" + thief);
              crashPlace(8, 1, 0);
              uncountedAsyncAt(
                  place(thief),
                  () -> {
                    this.consolePrinter.remotePrintln(
                        victim, "Step 4 Give Random Loot to thief=" + thief);
                    if (crashNumber == 12) Thread.sleep(3000);
                    // this.consolePrinter.println("after at from=" + victim + " bagId=" +
                    // loot.bagId);
                    this.waitingLock.lock();
                    try {
                      boolean notRejected = this.deal(loot, victim, false);
                      if (notRejected) {
                        waiting.set(false);
                        waitingCondition.signalAll();
                      }
                    } finally {
                      this.waitingLock.unlock();
                    }
                  });
              crashPlace(12, 1, 0);
            });
      } catch (Throwable throwable) {
        throwable.printStackTrace(System.out);
      }
    } else {
      this.logger.lifelineStealsSuffered++;

      try {
        this.consolePrinter.println("Step 1 Give Lifeline Loot to thief=" + thief);
        asyncAt(
            here(),
            () -> {
              this.consolePrinter.println("Step 2 Give Lifeline Loot to thief=" + thief);
              this.writeStealBackup(loot, _currentSplits, thief, currentBag.bagId);
              this.consolePrinter.println("Step 3 Give Lifeline Loot to thief=" + thief);
              crashPlace(8, 1, 0);
              asyncAt(
                  place(thief),
                  () -> {
                    this.consolePrinter.remotePrintln(
                        victim, "Step 4 Give Lifeline Loot to thief=" + thief);
                    if (crashNumber == 11) Thread.sleep(3000);
                    this.deal(loot, victim, true);
                  });
              crashPlace(11, 1, 0);
            });
      } catch (Throwable throwable) {
        throwable.printStackTrace(System.out);
      }
    }
  }

  /**
   * Writes cyclic regular backups. Distribute works to (lifeline) thieves by calling the {@link
   * #give(LocalFTTaskBag)}
   */
  private void distribute() {
    synchronized (this.lifelineThieves) {
      if (this.thieves.size() + this.lifelineThieves.size() > 0) {
        this.logger.startStoppingTimeWithAutomaticEnd(FTLogger.COMMUNICATION);
      }

      LocalFTTaskBag randomLoot;

      // Random steals
      while (this.thieves.size() > 0 && this.descriptionRecords.size() > 0) {
        final Integer thief = this.thieves.poll();
        final int t = (thief >= 0) ? thief : (-thief - 1);
        final int descriptionRecord = this.descriptionRecords.remove(0);

        this.consolePrinter.println(
            "Give description record " + descriptionRecord + " to random thief " + t);

        uncountedAsyncAt(
            place(t),
            () -> {
              this.logger.startStoppingTimeWithAutomaticEnd(FTLogger.COMMUNICATION);
              this.waitingLock.lock();
              try {
                this.restoreWithReplayUnit(descriptionRecord);
                waiting.set(false);
                waitingCondition.signalAll();
              } finally {
                this.waitingLock.unlock();
              }
            });
      }

      // Random steals
      while (this.currentReplaySplits == null
          && this.thieves.size() > 0
          && (randomLoot = this.queue.split()) != null) {
        final Integer thief = this.thieves.poll();
        final int t = (thief >= 0) ? thief : (-thief - 1);

        this.consolePrinter.println("Give " + randomLoot.size() + " tasks to random thief " + t);

        give(randomLoot, t, false);
      }

      // lifeline steals
      while (this.lifelineThieves.size() > 0 && this.queue.size() > 1) {
        final Integer thief = this.lifelineThieves.poll();
        final int t = thief < 0 ? -thief - 1 : thief;

        final int v = here().id;

        this.consolePrinter.println("Start lifeline handshake with " + t);

        try {
          asyncAt(
              place(t),
              () -> {
                this.consolePrinter.remotePrintln(
                    v, "Lifeline handshake. Asking if tasks are needed.");
                try {

                  if (this.waitingForLifeline.getAndSet(true) == true) {
                    this.consolePrinter.remotePrintln(
                        v,
                        "Lifeline handshake. Another handshake already in progress with place="
                            + this.waitingForLifelinePlace.get());
                    this.lifelinesActivated[v] = true;
                    this.consolePrinter.remotePrintln(
                        v, "Lifeline handshake. Activating lifeline " + v);
                    uncountedAsyncAt(
                        place(v),
                        () -> {
                          synchronized (this.lifelineThieves) {
                            this.lifelineThieves.add(thief);
                            this.consolePrinter.println(
                                "Lifeline handshake. Add " + thief + " to lifelineThieves");
                            this.consolePrinter.println("Lifeline handshake. Aborted. thief=" + t);
                          }
                        });
                    return;
                  }
                  this.waitingForLifelinePlace.set(v);
                  this.consolePrinter.remotePrintln(
                      v, "Lifeline handshake. Set waitingForLifeline to true");

                  final LocalFTGLBResult result;
                  this.waitingLock.lock();
                  try {
                    while (this.waiting.get()) {
                      this.consolePrinter.remotePrintln(
                          v, "Lifeline handshake. Waiting for this.waiting");
                      try {
                        this.waitingCondition.await();
                      } catch (InterruptedException e) {
                        e.printStackTrace();
                      }
                    }
                    if (this.queue.size() > 0
                        || this.descriptionRecords.size() > 0) { // loot was already given
                      this.consolePrinter.remotePrintln(
                          v, "Lifeline handshake. Abort because Thief has tasks");
                      this.lifelinesActivated[v] = false;
                      this.consolePrinter.remotePrintln(
                          v, "Lifeline handshake. Deactivate lifeline " + v);
                      this.waitingForLifeline.set(false);
                      this.consolePrinter.remotePrintln(
                          v, "Lifeline handshake. Set waitingForLifeline to false");
                      return;
                    }
                    result = this.queue.getResult();
                  } finally {
                    this.waitingLock.unlock();
                  }
                  this.consolePrinter.remotePrintln(
                      v, "Lifeline handshake. Requesting actual tasks from victim");
                  asyncAt(
                      place(v),
                      () -> {
                        this.resultMap.put(t, result);
                        LocalFTTaskBag lifelineLoot;
                        this.consolePrinter.println(
                            "Lifeline handshake. Before this.waiting for thief=" + t);
                        this.waitingLock.lock();
                        try {
                          this.consolePrinter.println(
                              "Lifeline handshake. After this.waiting for thief=" + t);

                          if (this.descriptionRecords.size() > 0) {
                            final int descriptionRecord = this.descriptionRecords.remove(0);

                            this.consolePrinter.println(
                                "Lifeline handshake. Give description record "
                                    + descriptionRecord
                                    + " to thief="
                                    + t);

                            asyncAt(
                                place(t),
                                () -> {
                                  this.consolePrinter.println(
                                      "Lifeline handshake. Received description record "
                                          + descriptionRecord
                                          + " from victim = "
                                          + v);
                                  this.logger.startStoppingTimeWithAutomaticEnd(
                                      FTLogger.COMMUNICATION);
                                  this.restoreWithReplayUnit(descriptionRecord);
                                  final boolean oldActive = this.active.getAndSet(true);
                                  this.lifelinesActivated[v] = false;
                                  this.waitingForLifeline.set(false);
                                  if (oldActive == false) {
                                    this.processStack();
                                    this.logger.endStoppingTimeWithAutomaticEnd();
                                    this.logger.stopLive();
                                  }
                                  this.consolePrinter.println(
                                      "Lifeline handshake. Merged description record "
                                          + descriptionRecord
                                          + " from victim = "
                                          + v
                                          + "!");
                                });
                            return;
                          }
                          if (this.currentReplaySplits != null
                              || (lifelineLoot = this.queue.split()) == null
                              || lifelineLoot.size() == 0) {
                            this.consolePrinter.println(
                                "Lifeline handshake. Retry later because victim has no tasks. Thief="
                                    + t);
                            uncountedAsyncAt(
                                place(t),
                                () -> {
                                  this.waitingForLifeline.set(false);
                                  this.consolePrinter.println(
                                      "Lifeline handshake. Set waitingForLifeline to false");
                                  this.lifelinesActivated[v] = true;
                                  this.consolePrinter.println(
                                      "Lifeline handshake. Activate lifeline " + v);
                                });
                            synchronized (this.lifelineThieves) {
                              this.lifelineThieves.add(thief);
                              this.consolePrinter.println("Add " + thief + " to lifelineThieves");
                            }
                            return;
                          }
                          this.consolePrinter.println(
                              "Lifeline handshake. Give " + lifelineLoot.size() + " tasks to " + t);
                          this.give(lifelineLoot, t, true);
                        } finally {
                          this.waitingLock.unlock();
                        }
                      });
                } catch (Exception e) {
                  e.printStackTrace();
                  this.waitingForLifeline.set(false);
                  this.consolePrinter.println(
                      "Lifeline handshake. Exception: Set waitingForLifeline to false");
                  this.lifelinesActivated[v] = false;
                  this.consolePrinter.println(
                      "Lifeline handshake. Exception: Deactivate lifeline " + v);
                }
              });
        } catch (Throwable throwable) {
          throwable.printStackTrace();
        }
      }
    }
  }

  /**
   * Rejecting thieves when no task to share (or worth sharing). Note, never reject lifeline thief,
   * instead put it into the lifelineThieves stack,
   */
  private void reject() {
    synchronized (this.lifelineThieves) {
      this.logger.startStoppingTimeWithAutomaticEnd(FTLogger.COMMUNICATION);

      while (this.thieves.size() > 0) {
        final int thief = this.thieves.poll();
        this.consolePrinter.println("Remove " + thief + " from thieves");
        if (thief >= 0) {
          try {
            uncountedAsyncAt(
                place(thief),
                () -> {
                  this.waitingLock.lock();
                  try {
                    int newId = logger.startStoppingTime(FTLogger.COMMUNICATION);
                    this.waiting.set(false);
                    this.waitingCondition.signalAll();
                    this.logger.endStoppingTime(newId);
                  } finally {
                    this.waitingLock.unlock();
                  }
                });
          } catch (Throwable throwable) {
            throwable.printStackTrace(System.out);
          }
        } else {
          try {
            uncountedAsyncAt(
                place(-thief - 1),
                () -> {
                  this.waitingLock.lock();
                  try {
                    int newId = logger.startStoppingTime(FTLogger.COMMUNICATION);
                    this.waiting.set(false);
                    this.waitingCondition.signalAll();
                    this.logger.endStoppingTime(newId);
                  } finally {
                    this.waitingLock.unlock();
                  }
                });
          } catch (Throwable throwable) {
            throwable.printStackTrace(System.out);
          }
        }
      }
    }
  }

  /**
   * Send out steal requests. It does following things: (1) Probes w random victims and send out
   * stealing requests by calling into {@link #request(int, boolean)} (2) If probing random victims
   * fails, resort to lifeline buddies In both case, it sends out the request and wait on the
   * thieves' response, which either comes from or (ii) {@link #give(LocalFTTaskBag)}
   *
   * @return !empty.get();
   */
  public boolean steal() {
    if (places().size() == 1) {
      return false;
    }

    if (this.descriptionRecords.size() > 0) {
      int deadPlace = this.descriptionRecords.remove(0);
      this.consolePrinter.println(
          "Use Description Record " + deadPlace + " instead of sending a steal request");
      restoreWithReplayUnit(deadPlace);
      return true;
    }

    final LocalFTGLBResult result = this.queue.getResult();
    this.waitForBackups();

    this.logger.startStoppingTimeWithAutomaticEnd(FTLogger.COMMUNICATION);
    final int p = here().id;
    this.consolePrinter.println("waitingForLifeline=" + this.waitingForLifeline.get());
    for (int i = 0; i < this.w && this.empty.get() && !this.waitingForLifeline.get(); ++i) {
      this.logger.stealsAttempted++;
      this.waiting.set(true);
      this.logger.stopLive();

      if (this.m <= 0) {
        return false;
      }

      int v = this.victims[this.random.nextInt(this.m)];

      while (isDead(place(v))) {
        v = this.victims[this.random.nextInt(this.m)];
      }

      this.waitingPlace = v;

      try {
        this.consolePrinter.println("Send random steal request to " + v);
        uncountedAsyncAt(
            place(v),
            () -> {
              this.request(p, false, result);
            });
      } catch (Throwable throwable) {
        throwable.printStackTrace(System.out);
      }

      this.consolePrinter.println("before waiting1");
      this.waitingLock.lock();
      try {
        this.logger.startStoppingTimeWithAutomaticEnd(FTLogger.WAITING);
        while (this.waiting.get()) {
          try {
            this.consolePrinter.println("waiting for random steal from " + v + "!");
            this.waitingCondition.await();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      } finally {
        this.waitingLock.unlock();
      }
      this.consolePrinter.println("finished waiting for random steal from " + v + "!");

      this.logger.startLive();
    }

    for (int i = 0;
        (i < this.lifelines.length) && this.empty.get() && (0 <= this.lifelines[i]);
        ++i) {
      final int lifeline = this.lifelines[i];
      if (this.lifelinesActivated[lifeline] == false) {
        this.logger.lifelineStealsAttempted++;
        this.lifelinesActivated[lifeline] = true;
        this.consolePrinter.println("Activate lifeline " + lifeline);
        this.waiting.set(true);

        if (isDead(place(lifeline))) {
          continue;
        }

        this.waitingPlace = lifeline;

        try {
          this.consolePrinter.println("Send lifeline steal request to " + lifeline);
          uncountedAsyncAt(
              place(lifeline),
              () -> {
                this.request(p, true, null);
              });
        } catch (Throwable throwable) {
          throwable.printStackTrace(System.out);
        }

        this.consolePrinter.println("before waiting2");
        this.waitingLock.lock();
        try {
          this.logger.startStoppingTimeWithAutomaticEnd(FTLogger.WAITING);
          while (this.waiting.get()) {
            try {
              this.consolePrinter.println("waiting for lifeline steal from " + lifeline + "! ");
              this.waitingCondition.await();
            } catch (InterruptedException e) {
              e.printStackTrace(System.out);
            }
          }
        } finally {
          this.waitingLock.unlock();
        }
        this.consolePrinter.println("finished waiting for lifeline steal from " + lifeline + "!");
        this.logger.startLive();
      }
    }
    return !(this.empty.get());
  }

  /**
   * Remote thief sending requests to local LJR. When empty or waiting for more work, reject
   * non-lifeline thief right away. Note, never reject lifeline thief.
   *
   * @param thief place id of thief
   * @param lifeline if I am the lifeline buddy of the remote thief
   */
  public void request(int thief, boolean lifeline, LocalFTGLBResult result) {
    this.consolePrinter.println("Begin thief=" + thief + " lifeline=" + lifeline);
    try {
      synchronized (this.lifelineThieves) {
        this.consolePrinter.println("entered lifelineThieves");
        int newId = logger.startStoppingTime(FTLogger.COMMUNICATION);

        if (lifeline == false) this.resultMap.put(thief, result);

        if (this.thieves.contains(thief) || this.lifelineThieves.contains(thief)) {
          if (lifeline == true) {
            thieves.remove(thief);
            lifelineThieves.remove(thief);
            lifelineThieves.offer(thief);
            this.consolePrinter.println("Another request already exists. Move to lifelineThieves");
          }

          try {
            uncountedAsyncAt(
                place(thief),
                () -> {
                  this.waitingLock.lock();
                  try {
                    int thiefNewId = logger.startStoppingTime(FTLogger.COMMUNICATION);
                    this.waiting.set(false);
                    this.waitingCondition.signalAll();
                    this.logger.endStoppingTime(thiefNewId);
                  } finally {
                    this.waitingLock.unlock();
                  }
                });
          } catch (Throwable throwable) {
            throwable.printStackTrace(System.out);
          }
          this.logger.endStoppingTime(newId);
          return;
        }

        if (lifeline) {
          this.logger.lifelineStealsReceived++;
        } else {

          this.logger.stealsReceived++;
        }
        if (this.empty.get() || this.waiting.get()) {

          if (lifeline == true) {
            this.lifelineThieves.add(thief);
            this.consolePrinter.println("Add " + thief + " to lifelineThieves");
          }
          try {
            uncountedAsyncAt(
                place(thief),
                () -> {
                  this.waitingLock.lock();
                  try {
                    int thiefNewId = logger.startStoppingTime(FTLogger.COMMUNICATION);
                    this.waiting.set(false);
                    this.waitingCondition.signalAll();
                    this.logger.endStoppingTime(thiefNewId);
                  } finally {
                    this.waitingLock.unlock();
                  }
                });
          } catch (Throwable throwable) {
            throwable.printStackTrace(System.out);
          }
        } else {

          if (lifeline) {
            this.lifelineThieves.offer(thief);
            this.consolePrinter.println("Add " + thief + " to lifelineThieves");
            try {
              uncountedAsyncAt(
                  place(thief),
                  () -> {
                    this.waitingLock.lock();
                    try {
                      int thiefNewId = logger.startStoppingTime(FTLogger.COMMUNICATION);
                      this.waiting.set(false);
                      this.waitingCondition.signalAll();
                      this.logger.endStoppingTime(thiefNewId);
                    } finally {
                      this.waitingLock.unlock();
                    }
                  });
            } catch (Throwable throwable) {
              throwable.printStackTrace(System.out);
            }
          } else {
            this.thieves.offer(thief);
            this.consolePrinter.println("Add " + thief + " to thieves");
          }
        }

        this.logger.endStoppingTime(newId);
      }
    } catch (Throwable e) {
      e.printStackTrace(System.out);
    }
    this.consolePrinter.println("Leave thief=" + thief + " lifeline=" + lifeline);
  }

  /**
   * Merge current FTWorker'timestamps taskbag with incoming task bag.
   *
   * @param loot task bag to merge
   * @param lifeline if it is from a lifeline buddy
   */
  private void processLoot(LocalFTTaskBag loot, boolean lifeline, int source) {
    if (lifeline) {
      this.logger.lifelineStealsPerpetrated++;
      this.logger.lifelineNodesReceived += loot.size();
    } else {
      this.logger.stealsPerpetrated++;
      this.logger.nodesReceived += loot.size();
    }

    this.currentBag = loot;
    this.currentBackupId = 0;
    this.queue.merge(loot);
    this.empty.set(false);
  }

  private void waitForBackups() {
    this.logger.startWriteBackup();
    synchronized (this.runningBackups) {
      final int size = this.runningBackups.size();
      for (int i = 0; i < size; ++i) {
        try {
          this.runningBackups.removeFirst().get();
        } catch (Throwable t) {
          try {
            Thread.sleep(20);
          } catch (Throwable t2) {
            t2.printStackTrace();
          }
          t.printStackTrace();
        }
      }
    }
    this.logger.stopWriteBackup();
  }

  public void finishCurrentBag() {
    this.lastBagIds.add(currentBag.bagId);

    this.currentStep = 0;
    this.currentSplit = 0;
    this.currentSplits.clear();
  }

  /**
   * Main process function of FTWorker. It does 4 things: (1) execute at most n tasks (2) respond to
   * stealing requests (3) when not worth sharing tasks, reject the stealing requests (4) when
   * running out of tasks, steal from others
   */
  private void processStack() {
    if (-1 == workerStartTime) {
      workerStartTime = System.nanoTime();
    }

    this.consolePrinter.println("Entering process stack!");
    this.putWorkingPlaces(here(), true);

    this.logger.startStoppingTimeWithAutomaticEnd(FTLogger.PROCESSING);
    boolean cont;
    do {
      boolean process = false;
      do {
        long nano = 422000000000l;
        List<Integer> crashPlacesOne =
            Arrays.asList(12, 25, 38, 51, 64, 77, 90, 103, 116, 129, 142, 5);
        crashPlace(24, crashPlacesOne, nano);
        nano = 488000000000l;
        List<Integer> crashPlacesTwo =
            Arrays.asList(35, 22, 33, 44, 55, 66, 101, 88, 99, 110, 121, 132);
        crashPlace(24, crashPlacesTwo, nano);

        this.waitingLock.lock();
        try {
          this.logger.startStoppingTimeWithAutomaticEnd(FTLogger.PROCESSING);

          process = this.queue.process(this.n);
          crashPlace(1, 2, 0);
          crashPlace(2, 2, 0);
          crashPlace(3, 2, 0);
          crashPlace(4, 2, 0);

          if (crashNumber == 7) {
            for (Place p : places()) {
              if (p.id == 0) continue;
              try {
                uncountedAsyncAt(p, () -> System.exit(7));
              } catch (Throwable thr) {
                thr.printStackTrace();
              }
            }
          }

          currentStep += n;
          currentSplit = 0L;

          if (this.currentReplaySplits != null) {
            if (this.currentReplaySplits.size() > 0) {
              boolean appliedSplit;
              do {
                appliedSplit = false;
                LocalFTTaskBagSplit split = this.currentReplaySplits.get(0);
                this.consolePrinter.println(
                    "currentStep= " + currentStep + " split.step=" + split.step);
                if (split != null && split.step == currentStep) {
                  this.consolePrinter.println("Split the pool");
                  this.queue.split();
                  this.currentReplaySplits.remove(0);
                  appliedSplit = true;
                }
              } while (appliedSplit && this.currentReplaySplits.size() > 0);
            }
            if (this.currentReplaySplits.size() == 0) {
              this.consolePrinter.println("currentReplaySplits.size()=0");
              final long bagId = currentBag.bagId;
              this.currentBag = this.queue.getAllTasks();
              this.currentBag.ownerResult = this.queue.getResult();
              this.currentBag.bagId = bagId;
              this.currentBag.backupId = ++this.currentBackupId;

              final TransactionOptions txOptions =
                  new TransactionOptions().setTimeout(15, TimeUnit.SECONDS);
              while (true) {
                try {
                  this.hz.executeTransaction(
                      txOptions,
                      (TransactionalTaskContext context) -> {
                        final TransactionalList<Integer> transReplayList =
                            context.getList(replayListName);
                        final TransactionalMap<Integer, LocalFTTaskBag> transBagsMap =
                            context.getMap(bagsMapName);

                        transReplayList.remove(this.currentDescriptionRecord);
                        transBagsMap.set(getIMapKey(here().id), this.currentBag);

                        return null;
                      });
                  break;
                } catch (Exception e) {
                  e.printStackTrace();
                }
              }
              this.consolePrinter.println("Transaction over");
              this.currentReplaySplits = null;
              this.currentStep = 0;
              this.currentSplit = 0;
              this.currentSplits.clear();
            }
          }

          if (process == false) {
            if (this.currentBag != null) {
              finishCurrentBag();
              crashPlace(6, 2, 0);
            }
          }

          this.distribute();

          if (process == true && currentReplaySplits == null) {
            long period = System.nanoTime() - timestampLastBackup;
            if ((period / 1E9) >= this.s) {
              writeRegularBackup(false);
            }
          }

          this.logger.startStoppingTimeWithAutomaticEnd(FTLogger.PROCESSING);
        } finally {
          this.waitingLock.unlock();
        }

        this.reject();
      } while (process);

      this.consolePrinter.println("Outside of this.waiting");

      this.empty.set(true);

      this.reject();

      this.consolePrinter.println("Before Steal");
      this.waitingLock.lock();
      try {
        cont = this.steal() || 0 < this.queue.size();
        this.logger.startStoppingTimeWithAutomaticEnd(FTLogger.PROCESSING);
        this.active.set(cont);
      } finally {
        this.waitingLock.unlock();
      }
      this.consolePrinter.println("After steal");
    } while (cont);

    this.reject();
    this.logger.startStoppingTimeWithAutomaticEnd(FTLogger.IDLING);

    this.consolePrinter.println("before final backup");
    this.waitingLock.lock();
    try {
      this.writeRegularBackup(true);
    } finally {
      this.waitingLock.unlock();
    }

    this.consolePrinter.println("waiting for final backup");
    this.waitForBackups();
    this.consolePrinter.println("after final backup");

    this.putWorkingPlaces(here(), false);
    this.consolePrinter.println("Leaving process stack!");
  }

  private void writeRegularBackup(boolean finalBackup) {
    synchronized (this.runningBackups) {
      this.logger.startWriteBackup();
      while (this.blockBackups.get() == true) {
        try {
          this.runningBackups.wait();
        } catch (Throwable t) {
          t.printStackTrace();
        }
      }
      this.waitForBackups(); // guarantee maximum amount of lost steps is upper bound of the backup
      // interval

      final long bagId = currentBag.bagId;
      this.currentBag = this.queue.getAllTasks();
      this.currentBag.ownerResult = this.queue.getResult();
      this.currentBag.bagId = bagId;
      this.currentBag.backupId = ++this.currentBackupId;
      final LocalFTTaskBag bag = this.currentBag;
      final int h = here().id;
      try {
        this.runningBackups.addLast(
            this.iMapBags.submitToKey(
                getIMapKey(here().id),
                new EntryProcessor() {
                  @Override
                  public Object process(Map.Entry entry) {
                    LocalFTTaskBag myBag = (LocalFTTaskBag) entry.getValue();
                    if (myBag.bagId == bag.bagId) {
                      if (myBag.recovered == true) return null; // prevent late backups
                      entry.setValue(bag);
                    } else {
                    }
                    return null;
                  }

                  @Override
                  public EntryBackupProcessor getBackupProcessor() {
                    return this::process;
                  }
                }));
      } catch (Throwable t) {
        t.printStackTrace();
      }

      this.currentStep = 0;
      this.currentSplit = 0;
      this.currentSplits.clear();
      if (!finalBackup) this.logger.incrementBackupsWritten(FTLogger.REGBACKUP);
      else this.logger.incrementBackupsWritten(FTLogger.FINALBACKUP);
      this.timestampLastBackup = System.nanoTime();

      if (this.crashNumber == 10 && here().id == 2) {
        this.waitForBackups();
        crashPlace(10, 2, 0);
      }
    }
  }

  private void putWorkingPlaces(final Place place, final boolean state) {
    if (here().id == 0) {
      if (isDead(place) == true) {
        this.workingPlaces.put(place.id, false);
        return;
      }
      this.workingPlaces.put(place.id, (state && (isDead(place) == false)));
    } else {
      synchronized (this.waitingForPutWorking) {
        while (this.waitingForPutWorking.get() == true) {
          try {
            this.waitingForPutWorking.wait();
          } catch (InterruptedException e) {
            // this.consolePrinter.println(": " + e);
          }
        }
      }
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
                      synchronized (this.waitingForPutWorking) {
                        this.waitingForPutWorking.set(false);
                        this.waitingForPutWorking.notifyAll();
                      }
                    });
              } catch (Throwable t) {
                t.printStackTrace(System.out);
              }
            });
      } catch (Throwable throwable) {
        throwable.printStackTrace(System.out);
      }
    }
    // this.consolePrinter.println("end");
  }

  /**
   * Entry point when workload is only known dynamically . The workflow is terminated when (1) No
   * one has work to do (2) Lifeline steals are responded
   *
   * @param start init method used in {@link LocalFTWorker}, note the workload is not allocated,
   *     because the workload can only be self-generated.
   */
  public void main(Runnable start) {
    try {
      finish(
          () -> {
            try {
              this.restartDaemon();
              this.empty.set(false);
              this.active.set(true);
              this.logger.startLive();
              start.run();
              this.processStack();
              this.logger.endStoppingTimeWithAutomaticEnd();
              this.logger.stopLive();
            } catch (Throwable t) {
              t.printStackTrace(System.out);
            }
          });
    } catch (Throwable e) {
      System.out.println(here() + "(in main1): Throwable caught ");
      e.printStackTrace(System.out);
    }
  }

  private void restartPlaceWithDaemon(final Place place) {
    final int h = place.id;
    final Place p = place;

    if (here().id == 0) {
      if (isDead(p)) {
        return;
      }
      this.restartPlaces.add(h);

      synchronized (this.workingPlaces) {
        this.workingPlaces.notifyAll();
      }
    } else {
      try {
        uncountedAsyncAt(
            place(0),
            () -> {
              if (isDead(p)) {
                return;
              }

              this.restartPlaces.add(h);

              synchronized (this.workingPlaces) {
                this.workingPlaces.notifyAll();
              }
            });
      } catch (Throwable throwable) {
        throwable.printStackTrace(System.out);
      }
    }
  }

  /**
   * Only started on place 0. Last running activity. Is responsible for restart places in all
   * surrounding finish for correct termination.
   */
  private void restartDaemon() {
    final int h = here().id;
    this.workingPlaces.put(h, true);
    final int startPlacesSize = places().size();

    final ICompletableFuture futures[] = new ICompletableFuture[this.startPlacesSize];
    async(
        () -> {
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
              try {
                asyncAt(place(i), this::processStack);
              } catch (Throwable throwable) {
                throwable.printStackTrace(System.out);
              }
            }

            if (cont == true) {
              continue;
            }

            boolean openHandlerLeft = false;
            /*            for (Map.Entry<Integer, HashMap<Integer, Boolean>> entry :
                this.countHandler.entrySet()) {
              HashMap<Integer, Boolean> value = entry.getValue();
              for (Map.Entry<Integer, Boolean> e : value.entrySet()) {
                if (e.getValue() == false) {
                  openHandlerLeft = true;
                }
              }
            }

            cont |= !this.restartPlaces.isEmpty();
            cont |= openHandlerLeft;
            cont |= (this.countHandler.size() + places().size()) != startPlacesSize;
            */

            if (cont == true) {
              continue;
            }

            long beforeLoop = System.nanoTime();
            for (int i = 0; i < futures.length; ++i) {
              futures[i] =
                  this.iMapBags.submitToKey(
                      getIMapKey(i),
                      new ReadOnlyEntryProcessor() {
                        @Override
                        public Object process(Map.Entry entry) {
                          final LocalFTTaskBag bag = (LocalFTTaskBag) entry.getValue();
                          if ((bag.recovered == false && bag.size() > 0)) {
                            // || (bag.size() > 0 && bag.ownerResult == null)) {
                            return Boolean.TRUE;
                          } else {
                            return Boolean.FALSE;
                          }
                        }

                        @Override
                        public EntryBackupProcessor getBackupProcessor() {
                          return null;
                        }
                      });
            }
            for (int i = 0; i < futures.length; ++i) {
              if (futures[i].get().equals(Boolean.TRUE) == true) {
                cont = true;
                long now = System.nanoTime();
                if (now >= (workerStartTime + 2000000000000l)) {
                  this.consolePrinter.println("Place " + i + " still has tasks");
                  continue;
                }
                break;
              }
            }
            long afterLoop = System.nanoTime();
            System.out.println(
                here().id + " restartDaemon look took " + (afterLoop - beforeLoop) + " nanos!");

            if (this.replayList.size() > 0) {
              this.consolePrinter.println(
                  "Found remaining description records. size=" + this.replayList.size());
              this.descriptionRecords.clear();
              this.descriptionRecords.addAll(this.replayList);
              this.consolePrinter.println(
                  "New local record count= " + this.descriptionRecords.size());
              putWorkingPlaces(here(), true);
              restartPlaceWithDaemon(here());
              cont = true;
              continue;
            }
          }
          this.consolePrinter.println("EXITING RESTART DAEMON!");
        });
  }

  /**
   * Entry point when workload can be known statically. The workflow is terminated when (1) No one
   * has work to do (2) Lifeline steals are responded
   */
  public void main() {
    try {
      this.empty.set(false);
      this.active.set(true);
      this.logger.startLive();
      this.processStack();
      this.logger.endStoppingTimeWithAutomaticEnd();
      this.logger.stopLive();
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
  private boolean deal(LocalFTTaskBag loot, int source, boolean lifeline) {
    this.consolePrinter.println("deal from source=" + source + " lifeline=" + lifeline);
    this.logger.startStoppingTimeWithAutomaticEnd(FTLogger.COMMUNICATION);

    boolean oldActive;

    this.waitingLock.lock();
    try {
      if (this.ownedBags.contains(loot.bagId)) {
        this.consolePrinter.println("abort deal from source=" + source + " lifeline=" + lifeline);
        return false;
      }

      if (lifeline) {
        this.lifelinesActivated[source] = false;
        this.consolePrinter.println("Deactivate lifeline " + source);
      }

      oldActive = this.active.getAndSet(true);

      if (oldActive) {
        this.processLoot(loot, lifeline, source);
      } else {
        this.logger.startLive();
        this.processLoot(loot, lifeline, source);
      }

      if (lifeline == true) {
        this.waitingForLifeline.set(false);
        this.consolePrinter.println("Set waitingForLifeline to false");
      }

      this.ownedBags.add(loot.bagId);
      this.timestampLastBackup = System.nanoTime();
    } finally {
      this.waitingLock.unlock();
    }

    crashPlace(5, 2, 0);

    if (oldActive == false) {
      this.processStack();
      this.logger.endStoppingTimeWithAutomaticEnd();
      this.logger.stopLive();
    }
    this.consolePrinter.println("leave deal from source=" + source + " lifeline=" + lifeline);

    return true;
  }

  /** Kills all alive places and removes the partitionLostListeners */
  private void killAllPlaces() {
    this.iMapBags.removePartitionLostListener(this.iMapBagsHandlerRemoveID);
    this.iMapSplits.removePartitionLostListener(this.iMapSplitsHandlerRemoveID);
    GlobalRuntime.getRuntime().setPlaceFailureHandler((deadPlace) -> System.exit(40));
    GlobalRuntime.getRuntime().setRuntimeShutdownHandler(null);

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
                    this.iMapBags.removePartitionLostListener(this.iMapBagsHandlerRemoveID);
                    this.iMapSplits.removePartitionLostListener(this.iMapSplitsHandlerRemoveID);
                    GlobalRuntime.getRuntime()
                        .setPlaceFailureHandler((deadPlace) -> System.exit(44));
                    GlobalRuntime.getRuntime().setRuntimeShutdownHandler(null);
                    System.exit(44);
                  });
            } catch (Throwable throwable) {
            }
          }
        });
    System.exit(44);
  }

  private void crashPlace(int crashNumber, int place, long nanosAfterStart, long... opts) {
    if (crashNumber != this.crashNumber) {
      return;
    }
    long now = System.nanoTime();
    if (here().id == place && (now >= (workerStartTime + nanosAfterStart))) {
      String callerName = Thread.currentThread().getStackTrace()[2].getMethodName();

      System.out.println("now: " + now);
      System.out.println("workerStartTime: " + workerStartTime);
      System.out.println("nanosAfterStart: " + nanosAfterStart);
      System.out.println(
          "workerStartTime + nanosAfterStart: " + (workerStartTime + nanosAfterStart));

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

  private void crashPlace(
      int crashNumber, List<Integer> places, long nanosAfterStart, long... opts) {
    if (crashNumber != this.crashNumber) {
      return;
    }
    if (places.contains(here().id)) {
      crashPlace(crashNumber, here().id, nanosAfterStart, opts);
    }
  }
}
