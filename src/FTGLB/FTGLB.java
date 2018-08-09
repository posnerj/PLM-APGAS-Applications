package FTGLB;

import static apgas.Constructs.asyncAt;
import static apgas.Constructs.at;
import static apgas.Constructs.finish;
import static apgas.Constructs.here;
import static apgas.Constructs.places;

import apgas.GlobalRuntime;
import apgas.Place;
import apgas.SerializableCallable;
import apgas.impl.GlobalRuntimeImpl;
import apgas.util.PlaceLocalObject;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.transaction.TransactionalTaskContext;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import utils.Pair;

public class FTGLB<Queue extends FTTaskQueue<Queue, T>, T extends Serializable>
    implements Serializable {

  private static final long serialVersionUID = 1L;
  final transient IMap<Integer, QueueWrapper<Queue, T>> iMap;
  final transient HazelcastInstance hz = Hazelcast.getHazelcastInstanceByName("apgas");
  private final HashMap<Integer, Integer> placeKeyMap;
  long setupTime;
  long crunchNumberTime;
  long collectResultTime;
  FTGLBResult<T> rootGlbR;
  FTGLBParameters glbPara;
  FTWorker<Queue, T> worker;
  private int p = places().size();

  public FTGLB(SerializableCallable<Queue> init, FTGLBParameters glbPara, boolean tree) {
    this.glbPara = glbPara;
    this.setupTime = System.nanoTime();

    String openLootMapName = "iMapOpenLoot";
    MapConfig openLootMapConfig = new MapConfig(openLootMapName);
    openLootMapConfig.setBackupCount(glbPara.backupCount);
    this.hz.getConfig().addMapConfig(openLootMapConfig);
    IMap<Integer, HashMap<Integer, Pair<Long, TaskBag>>> iMapOpenLoot =
        this.hz.getMap(openLootMapName);

    String backupMapName = "iMapBackup";
    MapConfig backupMapConfig = new MapConfig(backupMapName);
    backupMapConfig.setBackupCount(glbPara.backupCount);
    this.hz.getConfig().addMapConfig(backupMapConfig);
    iMap = hz.getMap(backupMapName);

    while (hz.getPartitionService().isClusterSafe() == false) {
      System.out.println("waiting until cluster is safe...");
      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    Set<Partition> partitions = hz.getPartitionService().getPartitions();
    HashMap<Integer, Integer> placePartitionIdMap = new HashMap<>();
    for (int i = 0; i < places().size(); i++) {
      Member member = GlobalRuntimeImpl.getRuntime().getMembers().get(i);
      int pID = -1;
      for (Partition p : partitions) {
        if (p.getOwner().getUuid().equals(member.getUuid())) {
          pID = p.getPartitionId();
        }
      }
      placePartitionIdMap.put(i, pID);
    }

    this.placeKeyMap = new HashMap<>();
    for (int i = 0; i < places().size(); i++) {
      int pID = placePartitionIdMap.get(i);
      int key = 0;
      while (this.placeKeyMap.size() < places().size()) {
        Partition partition = hz.getPartitionService().getPartition(key);
        if (partition.getPartitionId() == pID) {
          this.placeKeyMap.put(i, key);
          break;
        }
        key++;
      }
    }

    SerializableCallable<FTWorker<Queue, T>> workerInit =
        () ->
            new FTWorker<Queue, T>(
                init,
                glbPara.n,
                glbPara.w,
                glbPara.l,
                glbPara.z,
                glbPara.m,
                tree,
                glbPara.timestamps,
                glbPara.k,
                glbPara.crashNumber,
                glbPara.backupCount,
                glbPara.P,
                this.placeKeyMap);

    System.out.println(places());

    this.worker = PlaceLocalObject.make(places(), workerInit);

    final long l = System.nanoTime();
    this.setupTime = l - this.setupTime;

    for (Place p : places()) {
      at(
          p,
          () -> {
            worker.logger.startStoppingTimeWithAutomaticEnd(FTLogger.IDLING);
          });
    }
  }

  public Queue getTaskQueue() {
    return worker.queue;
  }

  /**
   * Run method. This method is called when users does not know the workload upfront.
   *
   * @param start The method that (Root) initializes the workload that can start computation. Other
   *     places first get their workload by stealing.
   * @return {@link #collectResults(long)}
   */
  public T[] run(Runnable start) {
    crunchNumberTime = System.nanoTime();
    worker.main(start);
    long now = System.nanoTime();
    crunchNumberTime = now - crunchNumberTime;
    T[] r = collectResults(now);
    end(r);
    return r;
  }

  /**
   * Run method. This method is called when users can know the workload upfront and initialize the
   * workload in {@link FTTaskQueue}
   *
   * @return {@link #collectResults(long)}
   */
  public T[] runParallel() {
    crunchNumberTime = System.nanoTime();
    FTWorker.broadcast(worker);
    long now = System.nanoTime();
    crunchNumberTime = now - crunchNumberTime;
    T[] r = collectResults(now);
    end(r);
    return r;
  }

  /**
   * Print various IMap.FTGLB-related information, including result; time spent in initialization,
   * computation and result collection; any user specified log information (per place); and
   * IMap.FTGLB statistics.
   *
   * @param r result to println
   */
  private void end(T[] r) {
    // println result
    if (0 != (glbPara.v & FTGLBParameters.SHOW_RESULT_FLAG)) {
      rootGlbR.display(r);
    }
    // println overall timing information
    if (0 != (glbPara.v & FTGLBParameters.SHOW_TIMING_FLAG)) {
      System.out.println("Setup time:" + ((setupTime) / 1E9));
      System.out.println("Process time:" + ((crunchNumberTime) / 1E9));
      System.out.println("Result reduce time:" + (collectResultTime / 1E9));
    }

    // println log
    if (0 != (glbPara.v & FTGLBParameters.SHOW_TASKFRAME_LOG_FLAG)) {
      printLog();
    }

    // collect glb statistics and println it out
    if (0 != (glbPara.v & FTGLBParameters.SHOW_GLB_FLAG)) {
      collectLifelineStatus();
    }
  }

  /** Collect IMap.FTGLB statistics */
  private void collectLifelineStatus() {
    FTLogger[] logs;
    final int V = this.glbPara.v;
    final int P = p;
    final int S = this.glbPara.timestamps;

    if (1024 < p) {
      Function<Integer, FTLogger> filling =
          (Function<Integer, FTLogger> & Serializable)
              (Integer i) ->
                  at(
                      places().get(i * 32),
                      () -> {
                        final int h = here().id;
                        final int n = Math.min(32, P - h);

                        Function<Integer, FTLogger> newFilling =
                            (Function<Integer, FTLogger> & Serializable)
                                (j ->
                                    at(
                                        places().get(h + j),
                                        () ->
                                            worker.logger.get(
                                                (V & FTGLBParameters.SHOW_GLB_FLAG) != 0)));

                        FTLogger[] newLogs = fillLogger(new FTLogger[n], newFilling);
                        FTLogger newLog = new FTLogger(S);
                        newLog.collect(newLogs);
                        return newLog;
                      });
      logs = fillLogger(new FTLogger[p / 32], filling);
    } else {
      int newP = places().size();
      logs = new FTLogger[newP];
      int i = 0;
      for (Place place : places()) {
        logs[i] = at(place, () -> worker.logger.get(true));
        i++;
      }
    }

    FTLogger log = new FTLogger(glbPara.timestamps);
    log.collect(logs);
    log.stats();

    try {
      log.printStoppedTime();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  protected T[] collectResults(long now) {
    this.collectResultTime = System.nanoTime();

    Queue result = null;

    final Collection<QueueWrapper<Queue, T>> values =
        hz.executeTransaction(
            (TransactionalTaskContext context) -> {
              return context.<Integer, QueueWrapper<Queue, T>>getMap("iMapBackup").values();
            });

    long tmpReduceCount = 0;
    final IMap<Integer, QueueWrapper<Queue, T>> tmpMap = hz.getMap("iMapBackup");
    for (int i = 0; i < tmpMap.size(); i++) {
      System.out.println(
          tmpMap.get(getBackupKey(i)).queue.getResult().getResult()[0]
              + ", "
              + tmpMap.get(getBackupKey(i)).queue.count()
              + ", "
              + tmpMap.get(getBackupKey(i)).queue.size());
      tmpReduceCount += tmpMap.get(getBackupKey(i)).queue.count();
    }

    System.out.println("tmpReduceCount: " + tmpReduceCount);

    System.out.println("openLoot: ");
    IMap<Integer, HashMap<Integer, Pair<Long, TaskBag>>> iMapOpenLoot = hz.getMap("iMapOpenLoot");

    for (int i = 0; i < iMapOpenLoot.size(); i++) {
      String tmp = i + ": ";
      for (Integer key : iMapOpenLoot.get(getBackupKey(i)).keySet()) {
        Pair<Long, TaskBag> pair = iMapOpenLoot.get(getBackupKey(i)).get(key);
        tmp += pair != null ? ("place: " + key + ", lid: " + pair.getFirst() + "; ") : "";
      }
      System.out.println(tmp);
    }

    for (final QueueWrapper<Queue, T> q : values) {
      if (result == null) {
        result = q.queue;
      } else {
        result.mergeResult(q.queue);
      }
    }

    finish(
        () -> {
          for (Place p : places()) {
            asyncAt(
                p,
                () -> {
                  worker.iMapBackup.removePartitionLostListener(worker.iMapBackupHandlerRemoveID);
                  worker.iMapOpenLoot.removePartitionLostListener(
                      worker.iMapOpenLootHandlerRemoveID);
                  GlobalRuntime.getRuntime().setPlaceFailureHandler((deadPlace) -> System.exit(88));
                });
          }
        });

    this.rootGlbR = result.getResult();
    this.collectResultTime = System.nanoTime() - collectResultTime;
    return result.getResult().getResult();
  }

  /**
   * Print logging information on each place if user is interested in collecting per place
   * information, i.e., statistics instrumented.
   */
  private void printLog() {
    int P = places().size();
    for (int i = 0; i < P; ++i) {
      at(places().get(i), () -> worker.queue.printLog());
    }
  }

  private FTLogger[] fillLogger(FTLogger[] arr, Function<Integer, FTLogger> function) {
    long now = System.nanoTime();
    for (int i = 0; i < arr.length; i++) {
      arr[i] = function.apply(i);
      final long l = System.nanoTime();
      now = l;
    }
    return arr;
  }

  private int getBackupKey(int placeID) {
    return this.placeKeyMap.get((placeID + 1) % p);
  }
}
