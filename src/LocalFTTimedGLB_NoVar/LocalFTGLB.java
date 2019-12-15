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
import apgas.impl.GlobalRuntimeImpl;
import apgas.util.GlobalRef;
import apgas.util.PlaceLocalObject;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.*;
import com.hazelcast.map.EntryBackupProcessor;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import utils.ReadOnlyEntryProcessor;

public class LocalFTGLB<Queue extends LocalFTTaskQueue<Queue, T>, T> implements Serializable {
  private static final long serialVersionUID = 1L;
  final transient IMap<Integer, LocalFTTaskBag> iMapBags;
  final transient IMap<Integer, ArrayList<LocalFTTaskBagSplit>> iMapSplits;
  final transient IList<Integer> replayList;
  final transient HazelcastInstance hz = Hazelcast.getHazelcastInstanceByName("apgas");
  long setupTime;
  long crunchNumberTime;
  long collectResultTime;
  LocalFTGLBResult<T> rootGlbR = null;
  LocalFTGLBParameters glbPara;
  LocalFTWorker<Queue, T> worker;
  private int p = places().size();
  private final HashMap<Integer, Integer> placeKeyMap;

  public LocalFTGLB(SerializableCallable<Queue> init, LocalFTGLBParameters glbPara, boolean tree) {
    this.glbPara = glbPara;
    this.setupTime = System.nanoTime();

    String bagsMapName = "iMapBags";
    MapConfig bagsMapConfig = new MapConfig(bagsMapName);
    bagsMapConfig.setBackupCount(glbPara.backupCount);
    bagsMapConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
    this.hz.getConfig().addMapConfig(bagsMapConfig);
    iMapBags = hz.getMap(bagsMapName);

    String splitsMapName = "iMapSplits";
    MapConfig splitsMapConfig = new MapConfig(splitsMapName);
    splitsMapConfig.setBackupCount(glbPara.backupCount);
    splitsMapConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
    this.hz.getConfig().addMapConfig(splitsMapConfig);
    iMapSplits = hz.getMap(splitsMapName);

    String replayListName = "replayList";
    ListConfig replayListConfig = new ListConfig(replayListName);
    replayListConfig.setBackupCount(glbPara.backupCount);
    replayList = this.hz.getList(replayListName);

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
        try {
          Partition partition = hz.getPartitionService().getPartition(key);
          if (null == partition) {
            System.err.println(
                "FTGLB constructor hz.getPartitionService().getPartition(" + key + ") is null!!!!");
          }

          if (null != partition && partition.getPartitionId() == pID) {
            this.placeKeyMap.put(i, key);
            break;
          }

        } catch (Throwable t) {
          t.printStackTrace();
        }
        key++;
      }
    }

    SerializableCallable<LocalFTWorker<Queue, T>> workerInit =
        () ->
            new LocalFTWorker<Queue, T>(
                init,
                glbPara.n,
                glbPara.w,
                glbPara.l,
                glbPara.z,
                glbPara.m,
                tree,
                glbPara.timestamps,
                glbPara.crashNumber,
                glbPara.backupCount,
                glbPara.P,
                this.placeKeyMap,
                glbPara.s);

    System.out.println(places());

    this.worker = PlaceLocalObject.make(places(), workerInit);

    final long l = System.nanoTime();
    this.setupTime = l - this.setupTime;

    //    for (Place p : places()) {
    //      at(
    //          p,
    //          () -> {
    //            worker.logger.startStoppingTimeWithAutomaticEnd(FTLogger.IDLING);
    //          });
    //    }
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
   * workload in {@link LocalFTTaskQueue}
   *
   * @return {@link #collectResults(long)}
   */
  public T[] runParallel() {
    crunchNumberTime = System.nanoTime();
    LocalFTWorker.broadcast(worker);
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
    if (0 != (glbPara.v & LocalFTGLBParameters.SHOW_RESULT_FLAG)) {
      rootGlbR.display(r);
    }
    // println overall timing information
    if (0 != (glbPara.v & LocalFTGLBParameters.SHOW_TIMING_FLAG)) {
      System.out.println("Setup time:" + ((setupTime) / 1E9));
      System.out.println("Process time:" + ((crunchNumberTime) / 1E9));
      System.out.println("Result reduce time:" + (collectResultTime / 1E9));
    }

    // println log
    if (0 != (glbPara.v & LocalFTGLBParameters.SHOW_TASKFRAME_LOG_FLAG)) {
      printLog();
    }

    // collect glb statistics and println it out
    if (0 != (glbPara.v & LocalFTGLBParameters.SHOW_GLB_FLAG)) {
      collectLifelineStatus();
    }
  }

  /** Collect IMap.FTGLB statistics */
  private void collectLifelineStatus() {
    final GlobalRef<FTLogger[]> logs = new GlobalRef<>(new FTLogger[p]);

    finish(
        () -> {
          for (Place p : places()) {
            asyncAt(
                p,
                () -> {
                  worker.logger.stoppingTimeToResult();
                  final FTLogger logRemote = worker.logger.get();
                  final int idRemote = here().id;
                  asyncAt(
                      logs.home(),
                      () -> {
                        logs.get()[idRemote] = logRemote;
                      });
                });
          }
        });

    for (final FTLogger l : logs.get()) {
      System.out.println(l);
    }

    FTLogger log = new FTLogger(glbPara.timestamps);
    log.collect(logs.get());
    log.stats(crunchNumberTime);

    try {
      log.printStoppedTime();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  protected T[] collectResults(long now) {
    this.collectResultTime = System.nanoTime();

    /*        final Collection<HashMap<Long, TaskBag>> values = hz.executeTransaction(
                    (TransactionalTaskContext context) -> {
                        return context.<Integer, HashMap<Long, TaskBag>>getMap("iMapBags").values();
                    }
            );
    */

    do {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    } while (!this.hz.getPartitionService().isClusterSafe());

    final ICompletableFuture futures[] = new ICompletableFuture[p];

    for (int i = 0; i < futures.length; ++i) {
      futures[i] =
          this.iMapBags.submitToKey(
              getBackupKey(i),
              new ReadOnlyEntryProcessor() {
                @Override
                public Object process(Map.Entry entry) {
                  return entry.getValue();
                }

                @Override
                public EntryBackupProcessor getBackupProcessor() {
                  return null;
                }
              });
    }

    LocalFTGLBResult<T> result = null;
    for (final ICompletableFuture f : futures) {
      if (null == result) {
        try {
          final LocalFTTaskBag bag = (LocalFTTaskBag) f.get();
          if (bag != null && bag.ownerResult != null) result = bag.ownerResult;
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (ExecutionException e) {
          e.printStackTrace();
        }
        continue;
      } else {
        try {
          final LocalFTTaskBag bag = (LocalFTTaskBag) f.get();
          if (bag != null && bag.ownerResult != null) result.mergeResult(bag.ownerResult);
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (ExecutionException e) {
          e.printStackTrace();
        }
      }
    }

    finish(
        () -> {
          for (Place p : places()) {
            asyncAt(
                p,
                () -> {
                  worker.iMapBags.removePartitionLostListener(worker.iMapBagsHandlerRemoveID);
                  worker.iMapSplits.removePartitionLostListener(worker.iMapSplitsHandlerRemoveID);
                  GlobalRuntime.getRuntime().setPlaceFailureHandler((deadPlace) -> System.exit(88));
                });
          }
        });

    this.rootGlbR = result;
    this.collectResultTime = System.nanoTime() - collectResultTime;
    return result.getResult();
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
