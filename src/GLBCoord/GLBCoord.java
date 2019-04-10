package GLBCoord;

import static apgas.Constructs.asyncAt;
import static apgas.Constructs.finish;
import static apgas.Constructs.here;
import static apgas.Constructs.place;
import static apgas.Constructs.places;

import GLBCoop.GLBParameters;
import GLBCoop.GLBResult;
import GLBCoop.Logger;
import apgas.Place;
import apgas.SerializableCallable;
import apgas.util.GlobalRef;
import java.io.IOException;
import java.io.Serializable;
import utils.ConsolePrinter;

public class GLBCoord<Queue extends TaskQueueCoord<Queue, T>, T extends Serializable>
    implements Serializable {

  private static final long serialVersionUID = 1L;
  long setupTime;
  long crunchNumberTime;
  long collectResultTime;
  GLBResult<T> rootGlbR = null;
  GLBParameters glbPara;
  ConsolePrinter consolePrinter;
  private int p = places().size();
  private GlobalRef<WorkerCoord<Queue, T>> globalRef;

  public GLBCoord(SerializableCallable<Queue> init, GLBParameters glbPara, boolean tree) {
    consolePrinter = ConsolePrinter.getInstance();
    consolePrinter.println("[Cooperative.FTGLB " + here() + "]: entering constructor.");
    this.glbPara = glbPara;
    this.setupTime = System.nanoTime();

    SerializableCallable<WorkerCoord<Queue, T>> workerInit =
        () ->
            new WorkerCoord<Queue, T>(
                init,
                glbPara.n,
                glbPara.w,
                glbPara.l,
                glbPara.z,
                glbPara.m,
                tree,
                glbPara.timestamps);
    globalRef = new GlobalRef<>(places(), workerInit);

    final long l = System.nanoTime();
    this.setupTime = l - this.setupTime;

    //    for (Place p : places()) {
    //      at(
    //          p,
    //          () -> {
    //            globalRef.get().logger.timeReference = l;
    //            globalRef.get().logger.startStoppingTimeWithAutomaticEnd(Logger.DEAD);
    //          });
    //    }

    consolePrinter.println("[GLBCoord.SplitGLB " + here() + "]: leaving constructor.");
  }

  public Queue getTaskQueue() {
    return globalRef.get().queue;
  }

  /**
   * Run method. This method is called when users does not know the workload upfront.
   *
   * @param start The method that (Root) initializes the workload that can start computation. Other
   *     places first get their workload by stealing.
   * @return {@link #collectResults()}
   */
  public T[] run(Runnable start) {
    consolePrinter.println("[GLBCoord.SplitGLB " + here() + "]: starting with run().");
    crunchNumberTime = System.nanoTime();
    globalRef.get().main(globalRef, start);
    long now = System.nanoTime();
    crunchNumberTime = now - crunchNumberTime;
    consolePrinter.println("[GLBCoord.SplitGLB " + here() + "]: reducing result.");
    T[] r = collectResults();
    end(r);
    return r;
  }

  /**
   * Run method. This method is called when users can know the workload upfront and initialize the
   * workload in {@link TaskQueueCoord}
   *
   * @return {@link #collectResults()}
   */
  public T[] runParallel() {
    consolePrinter.println("[GLBCoord.SplitGLB " + here() + "]: starting with runParallel().");
    crunchNumberTime = System.nanoTime();
    WorkerCoord.broadcast(globalRef);
    long now = System.nanoTime();
    crunchNumberTime = now - crunchNumberTime;
    consolePrinter.println("[GLBCoord.SplitGLB " + here() + "]: reducing result.");
    T[] r = collectResults();
    end(r);
    return r;
  }

  /**
   * Print various Cooperative.FTGLB-related information, including result; time spent in
   * initialization, computation and result collection; any user specified log information (per
   * place); and Cooperative.FTGLB statistics.
   *
   * @param r result to println
   */
  private void end(T[] r) {
    // println result
    if (0 != (glbPara.v & GLBParameters.SHOW_RESULT_FLAG)) {
      rootGlbR.display(r);
    }

    if (0 != (glbPara.v & GLBParameters.SHOW_TIMING_FLAG)) {
      System.out.println("Setup time:" + ((setupTime) / 1E9));
      System.out.println("Process time:" + ((crunchNumberTime) / 1E9));
      System.out.println("Result reduce time:" + (collectResultTime / 1E9));
    }

    if (0 != (glbPara.v & GLBParameters.SHOW_TASKFRAME_LOG_FLAG)) {
      //      printLog();
    }

    if (0 != (glbPara.v & GLBParameters.SHOW_GLB_FLAG)) {
      long now = System.nanoTime();
      collectLifelineStatus(globalRef);
      System.out.println("Collect Lifelinestatus time:" + ((System.nanoTime() - now) / 1E9));
    }
  }

  /**
   * Collect Cooperative.FTGLB statistics
   *
   * @param globalRef PlaceLocalHandle for {@link WorkerCoord}
   */
  private void collectLifelineStatus(GlobalRef<WorkerCoord<Queue, T>> globalRef) {
    final GlobalRef<Logger[]> logs = new GlobalRef<>(new Logger[p]);

    finish(
        () -> {
          for (Place p : places()) {
            asyncAt(
                p,
                () -> {
                  globalRef.get().logger.stoppingTimeToResult();
                  final Logger logRemote = globalRef.get().logger.get();
                  final int idRemote = here().id;
                  asyncAt(
                      logs.home(),
                      () -> {
                        logs.get()[idRemote] = logRemote;
                      });
                });
          }
        });

    for (final Logger l : logs.get()) {
      System.out.println(l);
    }

    Logger log = new Logger(glbPara.timestamps);
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

    GlobalRef<TaskQueueCoord[]> globalResults =
        new GlobalRef<>(new TaskQueueCoord[places().size()]);
    finish(
        () -> {
          for (final Place p : places()) {
            asyncAt(
                p,
                () -> {
                  final Queue qRemote = this.globalRef.get().queue;
                  final int idRemote = here().id;
                  asyncAt(
                      globalResults.home(),
                      () -> {
                        globalResults.get()[idRemote] = qRemote;
                      });
                });
          }
        });

    TaskQueueCoord result = null;
    for (final TaskQueueCoord q : globalResults.get()) {
      if (null == result) {
        result = q;
        continue;
      }
      result.mergeResult(q);
    }
    this.rootGlbR = result.getResult();
    collectResultTime = System.nanoTime() - collectResultTime;
    return this.rootGlbR.getResult();
  }

  /**
   * Print logging information on each place if user is interested in collecting per place
   * information, i.e., statistics instrumented.
   *
   * @param globalRef GlobalRef for {@link WorkerCoord}
   */
  private void printLog(GlobalRef<WorkerCoord<Queue, T>> globalRef) {
    int P = places().size();
    finish(
        () -> {
          for (int i = 0; i < P; ++i) {
            asyncAt(place(i), () -> globalRef.get().queue.printLog());
          }
        });
  }
}
