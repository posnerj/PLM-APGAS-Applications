package FTGLB;

import static apgas.Constructs.async;
import static apgas.Constructs.asyncAt;
import static apgas.Constructs.at;
import static apgas.Constructs.finish;
import static apgas.Constructs.here;
import static apgas.Constructs.isDead;
import static apgas.Constructs.place;
import static apgas.Constructs.places;
import static apgas.Constructs.prevPlace;
import static apgas.Constructs.uncountedAsyncAt;

import GLBCoop.TaskBag;
import apgas.DeadPlacesException;
import apgas.GlobalRuntime;
import apgas.Place;
import apgas.SerializableCallable;
import apgas.util.ExactlyOnceExecutor;
import apgas.util.PlaceLocalObject;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
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
import utils.ReadOnlyEntryProcessor;

/**
 * The local runner for the Cooperative.FTGLB framework. An instance of this class runs at each
 * place and provides the context within which user-specified tasks execute and are load balanced
 * across all places.
 *
 * @param <Queue> Concrete TaskQueue type
 * @param <T> Result type.
 */
public final class FTWorker<Queue extends FTTaskQueue<Queue, T>, T extends Serializable>
    extends PlaceLocalObject implements Serializable {

  private static final long serialVersionUID = 1L;
  /** the resilient map for backups */
  protected final transient IMap<Integer, QueueWrapper<Queue, T>> iMapBackup;

  protected final String iMapBackupHandlerRemoveID;
  /** given loot without answer */
  protected final transient IMap<Integer, HashMap<Integer, Pair<Long, TaskBag>>> iMapOpenLoot;

  protected final String iMapOpenLootHandlerRemoveID;
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
  private final long k;

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
  /** TaskQueue, responsible for crunching numbers */
  Queue queue;

  QueueWrapper<Queue, T> queueWrapper;
  /** Logger to record the work-stealing status */
  FTLogger logger;

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

  /** Thieves that send stealing requests */
  private ArrayList<Integer> deadPlaces;
  //    private final IAtomicLong iCurrentLid;

  /** Waiting for response of this place */
  private int waitingPlace;

  /** Name of current iMapBackup for backups */
  private String backupMapName;

  /** Name of current iMapBackup for openLoots */
  private String openLootMapName;

  /** Write cyclic backups every k * n computation-elements. */
  private long currentK;

  /**
   * Class constructor
   *
   * @param init function closure to init the local {@link FTWorker}
   * @param n same to this.n
   * @param w same to this.w
   * @param l power of lifeline graph
   * @param z base of lifeline graph
   * @param m same to this.m
   * @param tree true if the workload is dynamically generatedf, false if the workload can be
   *     statically generated
   * @param s true if stopping Time in Logger, false if not
   */
  FTWorker(
      SerializableCallable<Queue> init,
      int n,
      int w,
      int l,
      int z,
      int m,
      boolean tree,
      int s,
      long k,
      int crashNumber,
      int backupCount,
      int P,
      HashMap<Integer, Integer> placeKeyMap) {
    this.startPlacesSize = P;
    this.placeKeyMap = placeKeyMap;

    this.n = n;
    this.w = w;
    this.m = m;
    this.k = k;
    this.waitingForPutWorking = new AtomicBoolean(false);
    this.currentLid = Long.MIN_VALUE;
    this.currentLid++;
    this.logger = new FTLogger(s);

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
        HashMap<Integer, Pair<Long, TaskBag>> newHashMap = new HashMap<>();
        this.iMapOpenLoot.set(getBackupKey(i), newHashMap);
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

    this.backupMapName = "iMapBackup";

    MapConfig backupMapConfig = new MapConfig(this.backupMapName);
    backupMapConfig.setBackupCount(backupCount);
    this.hz.getConfig().addMapConfig(backupMapConfig);

    this.iMapBackup = hz.getMap(this.backupMapName);
    this.logger.incrementBackupsWritten(FTLogger.INITBACKUP);

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

    try {
      this.queue = init.call();
      this.queueWrapper = new QueueWrapper<Queue, T>(queue, startPlacesSize);
      this.iMapBackup.set(getBackupKey(here().id), this.queueWrapper);
    } catch (Throwable throwable) {
      throwable.printStackTrace(System.out);
    }

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
  }

  /**
   * Internal method used by {@link FTGLB} to start FTWorker at each place when the workload is
   * known statically.
   */
  static <Queue extends FTTaskQueue<Queue, T>, T extends Serializable> void broadcast(
      FTWorker<Queue, T> worker) {
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
    final Place placeFailureHandlerPlace = here();

    if (deadPlace.id == 0) {
      this.killAllPlaces();
      return;
    }

    boolean isBackupPlace = false;
    boolean oldActive = true;
    boolean merged = false;

    synchronized (this.waiting) {
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
      }

      synchronized (this.lifelineThieves) {
        boolean removeLifelineThieves = this.lifelineThieves.remove(Integer.valueOf(deadPlace.id));
        boolean removeLifeLineFromThieves = this.thieves.remove(Integer.valueOf(deadPlace.id));
        boolean removeThieves = this.thieves.remove(Integer.valueOf(-deadPlace.id - 1));
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
        synchronized (this.waiting) {
          this.waiting.set(false);
          this.waiting.notifyAll();
        }
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
          isBackupPlace |= restoreBackup(iteratorDeadPlace);

          boolean foundLoot = false;
          final HashMap<Integer, Pair<Long, TaskBag>> deadPlaceOpenLoot =
              this.iMapOpenLoot.get(getBackupKey(iteratorDeadPlace.id));
          for (Pair<Long, TaskBag> pair : deadPlaceOpenLoot.values()) {
            if (pair != null) {
              foundLoot = true;
              break;
            }
          }

          if (foundLoot == false) {
            iteratorDeadPlace = place((iteratorDeadPlace.id + 1) % this.startPlacesSize);
            continue;
          }

          // openLoot was found
          for (Map.Entry<Integer, Pair<Long, TaskBag>> entry : deadPlaceOpenLoot.entrySet()) {
            if (entry.getValue() == null) {
              continue;
            }
            final int thiefOfDeadPlaceID = entry.getKey();
            final Place thiefOfDeadPlace = place(thiefOfDeadPlaceID);
            final Long deadLid = entry.getValue().getFirst();
            final TaskBag deadLoot = entry.getValue().getSecond();
            final int iteratorDeadPlaceID = iteratorDeadPlace.id;

            if (isDead(thiefOfDeadPlace)) {
              if (this.iMapBackup
                          .get(getBackupKey(thiefOfDeadPlaceID))
                          .getReceivedLid(iteratorDeadPlaceID)
                      >= deadLid
                  || this.iMapBackup.get(getBackupKey(iteratorDeadPlaceID)).getMyLid() < deadLid) {
                deadPlaceOpenLoot.put(thiefOfDeadPlaceID, null);
                this.iMapOpenLoot.set(getBackupKey(iteratorDeadPlaceID), deadPlaceOpenLoot);
              } else {
                try {
                  TransactionOptions txOptions =
                      new TransactionOptions().setTimeout(15, TimeUnit.SECONDS);
                  this.logger.startWriteBackup();
                  this.hz.executeTransaction(
                      txOptions,
                      (TransactionalTaskContext context) -> {
                        final TransactionalMap<Integer, QueueWrapper<Queue, T>> transBackupMap =
                            context.getMap(this.backupMapName);
                        final TransactionalMap<Integer, HashMap<Integer, Pair<Long, TaskBag>>>
                            transOpenLootMap = context.getMap(this.openLootMapName);
                        this.queue.merge(deadLoot);
                        transBackupMap.set(getBackupKey(here().id), this.queueWrapper);
                        this.logger.incrementBackupsWritten(FTLogger.STEALBACKUP);
                        this.currentK = 0;
                        deadPlaceOpenLoot.put(thiefOfDeadPlaceID, null);
                        transOpenLootMap.set(getBackupKey(iteratorDeadPlaceID), deadPlaceOpenLoot);

                        QueueWrapper<Queue, T> deadPlaceBackup =
                            transBackupMap.get(getBackupKey(iteratorDeadPlaceID));
                        deadPlaceBackup.setDone(true);
                        transBackupMap.set(getBackupKey(iteratorDeadPlaceID), deadPlaceBackup);
                        return null;
                      });
                  this.logger.stopWriteBackup();
                  merged = true;
                } catch (TransactionException transException) {
                  transException.printStackTrace(System.out);
                } catch (Throwable t) {
                  t.printStackTrace(System.out);
                }
              }
            } else {
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

                deadPlaceOpenLoot.put(thiefOfDeadPlaceID, null);
                this.iMapOpenLoot.set(getBackupKey(iteratorDeadPlaceID), deadPlaceOpenLoot);

                if (thiefOldActive == false) {
                  this.restartPlaceWithDaemon(thiefOfDeadPlace);
                }

              } catch (Throwable t) {
                t.printStackTrace(System.out);

                if (this.iMapBackup
                            .get(getBackupKey(thiefOfDeadPlaceID))
                            .getReceivedLid(iteratorDeadPlaceID)
                        >= deadLid
                    || this.iMapBackup.get(getBackupKey(iteratorDeadPlaceID)).getMyLid()
                        < deadLid) {
                  deadPlaceOpenLoot.put(thiefOfDeadPlaceID, null);
                  this.iMapOpenLoot.set(getBackupKey(iteratorDeadPlaceID), deadPlaceOpenLoot);
                } else {
                  try {
                    TransactionOptions txOptions =
                        new TransactionOptions().setTimeout(15, TimeUnit.SECONDS);
                    this.logger.startWriteBackup();
                    this.hz.executeTransaction(
                        txOptions,
                        (TransactionalTaskContext context) -> {
                          final TransactionalMap<Integer, QueueWrapper<Queue, T>> transBackupMap =
                              context.getMap(this.backupMapName);
                          final TransactionalMap<Integer, HashMap<Integer, Pair<Long, TaskBag>>>
                              transOpenLootMap = context.getMap(this.openLootMapName);

                          this.queue.merge(deadLoot);
                          transBackupMap.set(getBackupKey(here().id), this.queueWrapper);
                          this.logger.incrementBackupsWritten(FTLogger.STEALBACKUP);
                          this.currentK = 0;
                          deadPlaceOpenLoot.put(thiefOfDeadPlaceID, null);

                          transOpenLootMap.set(
                              getBackupKey(iteratorDeadPlaceID), deadPlaceOpenLoot);
                          QueueWrapper<Queue, T> deadPlaceBackup =
                              transBackupMap.get(getBackupKey(iteratorDeadPlaceID));
                          deadPlaceBackup.setDone(true);
                          transBackupMap.set(getBackupKey(iteratorDeadPlaceID), deadPlaceBackup);
                          return null;
                        });
                    this.logger.stopWriteBackup();
                    merged = true;
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
        HashMap<Integer, Pair<Long, TaskBag>> integerArrayListHashMap =
            this.iMapOpenLoot.get(getBackupKey(here().id));
        Pair<Long, TaskBag> deadPlaceLoot = integerArrayListHashMap.get(deadPlace.id);

        if (deadPlaceLoot != null && deadPlaceLoot.getFirst() > Long.MIN_VALUE) {
          final long backupLid =
              this.iMapBackup.get(getBackupKey(deadPlace.id)).getReceivedLid(here().id);

          if (deadPlaceLoot.getFirst() > backupLid) {
            try {
              TransactionOptions txOptions =
                  new TransactionOptions().setTimeout(15, TimeUnit.SECONDS);
              this.logger.startWriteBackup();
              this.hz.executeTransaction(
                  txOptions,
                  (TransactionalTaskContext context) -> {
                    final TransactionalMap<Integer, QueueWrapper<Queue, T>> transBackupMap =
                        context.getMap(this.backupMapName);
                    final TransactionalMap<Integer, HashMap<Integer, Pair<Long, TaskBag>>>
                        transOpenLootMap = context.getMap(this.openLootMapName);
                    this.queue.merge(deadPlaceLoot.getSecond());
                    transBackupMap.set(getBackupKey(here().id), this.queueWrapper);
                    this.logger.incrementBackupsWritten(FTLogger.STEALBACKUP);
                    this.currentK = 0;

                    integerArrayListHashMap.put(deadPlace.id, null);
                    transOpenLootMap.set(getBackupKey(here().id), integerArrayListHashMap);

                    QueueWrapper<Queue, T> deadPlaceBackup =
                        transBackupMap.get(getBackupKey(deadPlace.id));
                    deadPlaceBackup.setDone(true);
                    transBackupMap.set(getBackupKey(deadPlace.id), deadPlaceBackup);
                    return null;
                  });
              this.logger.stopWriteBackup();
              merged = true;
            } catch (TransactionException transException) {
              transException.printStackTrace(System.out);
            } catch (Throwable t) {
              t.printStackTrace(System.out);
            }
          } else {
            integerArrayListHashMap.put(deadPlace.id, null);
            this.iMapOpenLoot.set(getBackupKey(here().id), integerArrayListHashMap);
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

      if ((isBackupPlace == true || merged == true) && oldActive == false) {
        this.restartPlaceWithDaemon(here());
      }

      if (here().id == 0) {
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
        }
      }
    }
  }

  /**
   * Send notice to restartDeamon to restart place Only called from placeFailureHandler
   *
   * @param place place to restart
   */
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
   * Restores a backup
   *
   * @param restorePlace place which will be restored
   * @return really restored a backup?
   */
  private boolean restoreBackup(Place restorePlace) {
    boolean merged = false;

    try {
      TransactionOptions txOptions = new TransactionOptions().setTimeout(15, TimeUnit.SECONDS);
      merged =
          this.hz.executeTransaction(
              txOptions,
              (TransactionalTaskContext context) -> {
                final TransactionalMap<Integer, QueueWrapper<Queue, T>> transBackupMap =
                    context.getMap(this.backupMapName);
                QueueWrapper<Queue, T> deadQueue =
                    transBackupMap.getForUpdate(getBackupKey(restorePlace.id));
                TaskBag allTasks = deadQueue.queue.getAllTasks();
                deadQueue.queue.clearTasks();
                deadQueue.setDone(true);
                this.queue.merge(allTasks);

                transBackupMap.set(getBackupKey(here().id), this.queueWrapper);

                transBackupMap.set(getBackupKey(restorePlace.id), deadQueue);
                boolean result = false;
                if (this.queue.size() > 0 && allTasks.size() > 0) {
                  result = true;
                }
                this.currentK = 0;
                return result;
              });
    } catch (TransactionException transException) {
      transException.printStackTrace(System.out);
    } catch (Throwable throwable) {
      throwable.printStackTrace(System.out);
    }

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
  public void give(final TaskBag loot) {
    final long lid = this.currentLid++;
    final int victim = here().id;
    logger.nodesGiven += loot.size();
    final int h = here().id;

    if (this.thieves.size() > 0) {

      final Integer thief = this.thieves.poll();
      if (thief == null) {
        System.out.println(here() + "(in give): thief is null, should never happen!!! lid " + lid);
        this.queue.merge(loot);
        return;
      }

      final int t = thief < 0 ? -thief - 1 : thief;

      try {
        HashMap<Integer, Pair<Long, TaskBag>> hashMap = this.iMapOpenLoot.get(getBackupKey(h));
        Pair<Long, TaskBag> pair = new Pair<>(lid, loot);
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
      this.writeBackup(FTLogger.STEALBACKUP);

      if (thief >= 0) {
        this.logger.lifelineStealsSuffered++;
        try {
          uncountedAsyncAt(
              place(thief),
              () -> {
                synchronized (this.waiting) {
                  this.deal(loot, victim, lid, true);
                  this.waiting.set(false);
                  this.waiting.notifyAll();
                }
              });
        } catch (Throwable throwable) {
          throwable.printStackTrace(System.out);
        }
      } else {
        this.logger.stealsSuffered++;
        try {
          uncountedAsyncAt(
              place(-thief - 1),
              () -> {
                synchronized (this.waiting) {
                  this.deal(loot, victim, lid, false);
                  waiting.set(false);
                  waiting.notifyAll();
                }
              });
        } catch (Throwable throwable) {
          throwable.printStackTrace(System.out);
        }
      }
    } else {
      this.logger.lifelineStealsSuffered++;

      final Integer thief = this.lifelineThieves.poll();
      if (thief == null) {
        System.out.println(
            here() + "(in give2): thief is null, should never happen)" + " lid " + lid);
        this.queue.merge(loot);
        return;
      }
      final int t = thief < 0 ? -thief - 1 : thief;

      try {
        HashMap<Integer, Pair<Long, TaskBag>> hashMap = this.iMapOpenLoot.get(getBackupKey(h));
        Pair<Long, TaskBag> pair = new Pair<>(lid, loot);
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
      this.writeBackup(FTLogger.STEALBACKUP);

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
  }

  /**
   * Writes cyclic regular backups. Distribute works to (lifeline) thieves by calling the {@link
   * #give(TaskBag)}
   */
  private void distribute() {
    synchronized (this.lifelineThieves) {
      if (this.thieves.size() + this.lifelineThieves.size() > 0) {
        this.logger.startStoppingTimeWithAutomaticEnd(FTLogger.COMMUNICATION);
      } else {
        if (this.currentK >= this.k) {
          this.writeBackup(FTLogger.REGBACKUP);
        }
      }

      TaskBag loot;
      while (((this.thieves.size() > 0) || (this.lifelineThieves.size() > 0))
          && (loot = this.queue.split()) != null) {
        this.give(loot);
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
        if (thief >= 0) {
          this.lifelineThieves.add(thief);
          try {
            uncountedAsyncAt(
                place(thief),
                () -> {
                  synchronized (this.waiting) {
                    int newId = logger.startStoppingTime(FTLogger.COMMUNICATION);
                    this.waiting.set(false);
                    this.waiting.notifyAll();
                    this.logger.endStoppingTime(newId);
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
                  synchronized (this.waiting) {
                    int newId = logger.startStoppingTime(FTLogger.COMMUNICATION);
                    this.waiting.set(false);
                    this.waiting.notifyAll();
                    this.logger.endStoppingTime(newId);
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
   * thieves' response, which either comes from or (ii) {@link #give(TaskBag)}
   *
   * @return !empty.get();
   */
  public boolean steal() {
    if (this.startPlacesSize == 1) {
      return false;
    }

    this.logger.startStoppingTimeWithAutomaticEnd(FTLogger.COMMUNICATION);
    final int p = here().id;
    for (int i = 0; i < this.w && this.empty.get(); ++i) {
      this.logger.stealsAttempted++;
      this.waiting.set(true);
      this.logger.stopLive();

      if (this.m <= 0) {
        return false;
      }

      int v = this.victims[this.random.nextInt(this.m)];

      int count = 0;
      while (isDead(place(v))) {
        v = this.victims[this.random.nextInt(this.m)];
        count++;
        if (count >= 4) {
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
      } catch (Throwable throwable) {
        throwable.printStackTrace(System.out);
      }

      synchronized (this.waiting) {
        this.logger.startStoppingTimeWithAutomaticEnd(FTLogger.WAITING);
        while (this.waiting.get()) {
          try {
            this.waiting.wait();
          } catch (InterruptedException e) {
            System.out.println(here() + ": " + e);
          }
        }
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
        } catch (Throwable throwable) {
          throwable.printStackTrace(System.out);
        }

        synchronized (this.waiting) {
          this.logger.startStoppingTimeWithAutomaticEnd(FTLogger.WAITING);
          while (this.waiting.get()) {
            try {
              this.waiting.wait();
            } catch (InterruptedException e) {
              e.printStackTrace(System.out);
            }
          }
        }
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
  public void request(int thief, boolean lifeline) {
    final int t = thief < 0 ? -thief - 1 : thief;
    try {
      synchronized (this.lifelineThieves) {
        int newId = logger.startStoppingTime(FTLogger.COMMUNICATION);

        if (lifeline) {
          this.logger.lifelineStealsReceived++;
        } else {
          this.logger.stealsReceived++;
        }
        if (this.empty.get() || this.waiting.get()) {
          if (lifeline == true) {
            this.lifelineThieves.remove(Integer.valueOf(thief));
            if (this.iMapOpenLoot.get(getBackupKey(here().id)).get(t) == null) {
              this.lifelineThieves.add(thief);
            }
          }
          try {
            uncountedAsyncAt(
                place(thief),
                () -> {
                  synchronized (this.waiting) {
                    int thiefNewId = logger.startStoppingTime(FTLogger.COMMUNICATION);
                    this.waiting.set(false);
                    this.waiting.notifyAll();
                    this.logger.endStoppingTime(thiefNewId);
                  }
                });
          } catch (Throwable throwable) {
            throwable.printStackTrace(System.out);
          }
        } else {

          lifeline |= this.lifelineThieves.remove(Integer.valueOf(thief));
          if (this.iMapOpenLoot.get(getBackupKey(here().id)).get(t) == null) {
            if (lifeline) {
              this.thieves.offer(thief);
            } else {
              this.thieves.offer(-thief - 1);
            }
          } else {
            try {
              uncountedAsyncAt(
                  place(thief),
                  () -> {
                    synchronized (this.waiting) {
                      int thiefNewId = logger.startStoppingTime(FTLogger.COMMUNICATION);
                      this.waiting.set(false);
                      this.waiting.notifyAll();
                      this.logger.endStoppingTime(thiefNewId);
                    }
                  });
            } catch (Throwable throwable) {
              throwable.printStackTrace(System.out);
            }
          }
        }

        this.logger.endStoppingTime(newId);
      }
    } catch (Throwable e) {
      e.printStackTrace(System.out);
    }
  }

  /**
   * Merge current FTWorker'timestamps taskbag with incoming task bag.
   *
   * @param loot task bag to merge
   * @param lifeline if it is from a lifeline buddy
   */
  private void processLoot(TaskBag loot, boolean lifeline, long lid, int source) {
    if (lifeline) {
      this.logger.lifelineStealsPerpetrated++;
      this.logger.lifelineNodesReceived += loot.size();
    } else {
      this.logger.stealsPerpetrated++;
      this.logger.nodesReceived += loot.size();
    }
    this.queue.merge(loot);
    this.queueWrapper.setReceivedLid(source, lid);
    this.empty.set(false);
  }

  /**
   * Main process function of FTWorker. It does 4 things: (1) execute at most n tasks (2) respond to
   * stealing requests (3) when not worth sharing tasks, reject the stealing requests (4) when
   * running out of tasks, steal from others
   */
  private void processStack() {
    if (((here().id == 0)
            && (this.queue.count() == 1)
            && (this.workingPlaces.get(here().id) == true))
        == false) {
      this.putWorkingPlaces(here(), true);
    }

    this.logger.startStoppingTimeWithAutomaticEnd(FTLogger.PROCESSING);
    boolean cont;
    int size;
    do {
      boolean process = false;
      do {
        synchronized (this.waiting) {
          this.logger.startStoppingTimeWithAutomaticEnd(FTLogger.PROCESSING);

          process = this.queue.process(this.n);

          this.distribute();
          if (this.queue.size() > 0) {
            this.currentK++;
          }
          this.logger.startStoppingTimeWithAutomaticEnd(FTLogger.PROCESSING);
        }

        this.reject();
      } while (process);

      this.empty.set(true);

      this.reject();

      synchronized (this.waiting) {
        cont = this.steal() || 0 < this.queue.size();
        this.logger.startStoppingTimeWithAutomaticEnd(FTLogger.PROCESSING);
        this.active.set(cont);
        size = this.queue.size();
      }
    } while (cont);

    this.writeBackup(FTLogger.FINALBACKUP);

    this.reject();
    this.logger.startStoppingTimeWithAutomaticEnd(FTLogger.IDLING);

    this.putWorkingPlaces(here(), false);
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
            System.out.println(here() + ": " + e);
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
  }

  /**
   * Entry point when workload is only known dynamically . The workflow is terminated when (1) No
   * one has work to do (2) Lifeline steals are responded
   *
   * @param start init method used in {@link FTWorker}, note the workload is not allocated, because
   *     the workload can only be self-generated.
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
              this.logger.nodesCount = queue.count();
            } catch (Throwable t) {
              t.printStackTrace(System.out);
            }
          });
    } catch (DeadPlacesException e) {
      System.out.println(here() + "(in main1): DeadPlacesException caught");
      e.printStackTrace(System.out);
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
    final ICompletableFuture futures[] = new ICompletableFuture[startPlacesSize];

    async(
        () -> {
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

            long beforeLoop = System.nanoTime();
            for (int i = 0; i < futures.length; ++i) {
              futures[i] =
                  this.iMapOpenLoot.submitToKey(
                      getBackupKey(i),
                      new ReadOnlyEntryProcessor() {
                        @Override
                        public Object process(Map.Entry entry) {
                          final HashMap<Integer, Pair<Long, TaskBag>> map =
                              (HashMap<Integer, Pair<Long, TaskBag>>) entry.getValue();
                          for (Map.Entry<Integer, Pair<Long, TaskBag>> pairEntry : map.entrySet()) {
                            if (pairEntry.getValue() != null) {
                              return Boolean.TRUE;
                            }
                          }
                          return Boolean.FALSE;
                        }

                        @Override
                        public EntryBackupProcessor getBackupProcessor() {
                          return null;
                        }
                      });
            }
            for (int i = 0; i < futures.length; ++i) {
              if (futures[i].get().equals(Boolean.TRUE) == true) {
                countOpenLootLeft++;
                openHandlerLeft = true;
                ConsolePrinter.getInstance()
                    .println(here() + " restartDaemon:  Loot at key of place=" + i + " found");
                cont = true;
                break;
              }
            }
            long afterLoop = System.nanoTime();
            ConsolePrinter.getInstance()
                .println(
                    here()
                        + " restartDaemon:  loop took "
                        + ((afterLoop - beforeLoop) / 1E9)
                        + " cec!");

            if (cont == true || openHandlerLeft == true) {
              continue;
            }
          }
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
  private void deal(TaskBag loot, int source, long lid, boolean lifeline) {
    this.logger.startStoppingTimeWithAutomaticEnd(FTLogger.COMMUNICATION);

    final int h = here().id;
    final int s = source;

    boolean oldActive;

    synchronized (this.waiting) {
      if (this.queueWrapper.getReceivedLid(s) >= lid
          || this.iMapBackup.get(getBackupKey(s)).getMyLid() < lid) {
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

      this.writeBackup(FTLogger.STEALBACKUP);

      try {
        final int thiefID = here().id;
        final Place thief = here();
        final long lidToDelete = lid;
        uncountedAsyncAt(
            place(s),
            () -> {
              synchronized (this.waiting) {
                boolean found = false;
                HashMap<Integer, Pair<Long, TaskBag>> integerPairHashMap =
                    this.iMapOpenLoot.get(getBackupKey(here().id));
                Pair<Long, TaskBag> pair = integerPairHashMap.get(thiefID);
                if (pair != null && pair.getFirst() == lidToDelete) {
                  integerPairHashMap.put(thiefID, null);
                  this.iMapOpenLoot.set(getBackupKey(here().id), integerPairHashMap);
                  found = true;
                }
              }
            });
      } catch (Throwable throwable) {
        throwable.printStackTrace(System.out);
      }
    }

    if (oldActive == false) {
      this.processStack();
      this.logger.endStoppingTimeWithAutomaticEnd();
      this.logger.stopLive();
      this.logger.nodesCount = queue.count();
    }
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
  private boolean dealCalledFromHandler(TaskBag loot, int source, long lid) {
    final int s = source;

    boolean oldActive = false;
    synchronized (this.waiting) {
      if (this.queueWrapper.getReceivedLid(s) >= lid
          || this.iMapBackup.get(getBackupKey(s)).getMyLid() < lid) {
        return true;
      }

      oldActive = this.active.getAndSet(true);

      if (oldActive) {
        this.processLoot(loot, false, lid, source);
      } else {
        this.logger.startLive();
        this.processLoot(loot, false, lid, source);
      }

      this.writeBackup(FTLogger.STEALBACKUP);
    }
    return oldActive;
  }

  /**
   * Writes the hole queue to iMapBackup
   *
   * @param backupKind which kind of backup?
   */
  private void writeBackup(int backupKind) {
    this.logger.startStoppingTimeWithAutomaticEnd(FTLogger.COMMUNICATION);
    this.logger.startWriteBackup();
    synchronized (this.waiting) {
      try {
        final QueueWrapper queueToBackup = this.queueWrapper;

        this.exactlyOnceExecutor.executeOnKey(
            this.iMapBackup,
            getBackupKey(here().id),
            new EntryProcessor() {

              @Override
              public Object process(Map.Entry entry) {
                boolean done = ((QueueWrapper<Queue, T>) entry.getValue()).getDone();
                if (done == false) {
                  entry.setValue(queueToBackup);
                }
                return null;
              }

              @Override
              public EntryBackupProcessor getBackupProcessor() {
                return this::process;
              }
            });
        this.currentK = 0;
      } catch (Throwable throwable) {
        throwable.printStackTrace(System.out);
      }

      this.logger.incrementBackupsWritten(backupKind);
      this.logger.stopWriteBackup();
    }
  }

  /** Kills all alive places and removes the partitionLostListeners */
  private void killAllPlaces() {
    this.iMapBackup.removePartitionLostListener(this.iMapBackupHandlerRemoveID);
    this.iMapOpenLoot.removePartitionLostListener(this.iMapOpenLootHandlerRemoveID);
    GlobalRuntime.getRuntime().setPlaceFailureHandler((deadPlace) -> System.exit(40));

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
                    System.exit(44);
                  });
            } catch (Throwable throwable) {
            }
          }
        });
    System.exit(44);
  }
}
