/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package IncFTAsyncTimedGLB;

import static apgas.Constructs.asyncAt;
import static apgas.Constructs.at;
import static apgas.Constructs.finish;
import static apgas.Constructs.here;
import static apgas.Constructs.place;
import static apgas.Constructs.places;

import FTGLB.FTLogger;
import IncFTGLB.IncFTGLBResult;
import IncFTGLB.IncFTTaskQueue;
import IncFTGLB.IncQueueWrapper;
import IncFTGLB.IncTaskBag;
import apgas.GlobalRuntime;
import apgas.Place;
import apgas.SerializableCallable;
import apgas.impl.GlobalRuntimeImpl;
import apgas.util.GlobalRef;
import apgas.util.PlaceLocalObject;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.map.EntryBackupProcessor;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import utils.ConsolePrinter;
import utils.Pair;
import utils.ReadOnlyEntryProcessor;

public class IncFTAsyncTimedGLB<Queue extends IncFTTaskQueue<Queue, T>, T extends Serializable>
    implements Serializable {

  private static final long serialVersionUID = 1L;

  final transient HazelcastInstance hz = Hazelcast.getHazelcastInstanceByName("apgas");
  private final HashMap<Integer, Integer> placeKeyMap;
  long setupTime;
  long crunchNumberTime;
  long collectResultTime;
  IncFTGLBResult<T> rootGlbR = null;
  IncFTAsyncTimedGLBParameters glbPara;
  IncFTAsyncTimedWorker<Queue, T> worker;
  private int p = places().size();
  final transient IMap<Integer, IncQueueWrapper<Queue, T>> iMapBackup;

  public IncFTAsyncTimedGLB(
      SerializableCallable<Queue> init, IncFTAsyncTimedGLBParameters glbPara, boolean tree) {
    this.glbPara = glbPara;
    this.setupTime = System.nanoTime();

    String openLootMapName = "iMapOpenLoot";
    MapConfig openLootMapConfig = new MapConfig(openLootMapName);
    openLootMapConfig.setBackupCount(glbPara.backupCount);
    this.hz.getConfig().addMapConfig(openLootMapConfig);
    IMap<Integer, HashMap<Integer, Pair<Long, IncTaskBag>>> iMapOpenLoot =
        this.hz.getMap(openLootMapName);

    String backupMapName = "iMapBackup";
    MapConfig backupMapConfig = new MapConfig(backupMapName);
    backupMapConfig.setBackupCount(glbPara.backupCount);
    backupMapConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
    this.hz.getConfig().addMapConfig(backupMapConfig);
    iMapBackup = hz.getMap(backupMapName);

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

    SerializableCallable<IncFTAsyncTimedWorker<Queue, T>> workerInit =
        () ->
            new IncFTAsyncTimedWorker<Queue, T>(
                init,
                glbPara.n,
                glbPara.w,
                glbPara.l,
                glbPara.z,
                glbPara.m,
                tree,
                glbPara.timestamps,
                glbPara.s,
                glbPara.crashNumber,
                glbPara.backupCount,
                glbPara.P,
                this.placeKeyMap);

    ConsolePrinter.getInstance().println(placeKeyMap.toString());
    this.worker = PlaceLocalObject.make(places(), workerInit);

    while (hz.getPartitionService().isClusterSafe() == false) {
      System.out.println("waiting until cluster is safe 2...");
      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

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
   * @return {@link #collectResults()}
   */
  public T[] run(Runnable start) {
    crunchNumberTime = System.nanoTime();
    worker.main(start);
    long now = System.nanoTime();
    crunchNumberTime = now - crunchNumberTime;
    T[] r = collectResults();
    end(r);
    return r;
  }

  /**
   * Run method. This method is called when users can know the workload upfront and initialize the
   * workload in {@link IncFTTaskQueue}
   *
   * @return {@link #collectResults()}
   */
  public T[] runParallel() {
    crunchNumberTime = System.nanoTime();
    IncFTAsyncTimedWorker.broadcast(worker);
    long now = System.nanoTime();
    crunchNumberTime = now - crunchNumberTime;
    T[] r = collectResults();
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
    if (0 != (glbPara.v & IncFTAsyncTimedGLBParameters.SHOW_RESULT_FLAG)) {
      rootGlbR.display(r);
    }

    if (0 != (glbPara.v & IncFTAsyncTimedGLBParameters.SHOW_TIMING_FLAG)) {
      System.out.println("Setup time:" + ((setupTime) / 1E9));
      System.out.println("Process time:" + ((crunchNumberTime) / 1E9));
      System.out.println("Result reduce time:" + (collectResultTime / 1E9));
    }

    if (0 != (glbPara.v & IncFTAsyncTimedGLBParameters.SHOW_TASKFRAME_LOG_FLAG)) {
      //      printLog();
    }

    if (0 != (glbPara.v & IncFTAsyncTimedGLBParameters.SHOW_GLB_FLAG)) {
      long now = System.nanoTime();
      collectLifelineStatus();
      System.out.println("Collect Lifelinestatus time:" + ((System.nanoTime() - now) / 1E9));
    }
  }

  /** Collect IncFTTimedGLB statistics */
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

  protected T[] collectResults() {
    this.collectResultTime = System.nanoTime();

    Queue result = null;

    final ICompletableFuture futures[] = new ICompletableFuture[p];

    for (int i = 0; i < futures.length; ++i) {
      futures[i] =
          this.iMapBackup.submitToKey(
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

    for (final ICompletableFuture f : futures) {
      if (null == result) {
        try {
          result = ((IncQueueWrapper<Queue, T>) f.get()).queue;
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (ExecutionException e) {
          e.printStackTrace();
        }
        continue;
      } else {
        try {
          result.mergeResult(((IncQueueWrapper<Queue, T>) f.get()).queue);
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
    finish(
        () -> {
          for (int i = 0; i < P; ++i) {
            asyncAt(place(i), () -> worker.queue.printLog());
          }
        });
  }

  private int getBackupKey(int placeID) {
    return this.placeKeyMap.get((placeID + 1) % p);
  }
}
