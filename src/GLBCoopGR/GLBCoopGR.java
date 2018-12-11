package GLBCoopGR;

import static apgas.Constructs.asyncAt;
import static apgas.Constructs.at;
import static apgas.Constructs.finish;
import static apgas.Constructs.here;
import static apgas.Constructs.place;
import static apgas.Constructs.places;

import apgas.Place;
import apgas.SerializableCallable;
import apgas.util.GlobalRef;
import java.io.IOException;
import java.io.Serializable;
import utils.ConsolePrinter;

public class GLBCoopGR<Queue extends TaskQueueGR<Queue, T>, T extends Serializable>
    implements Serializable {

  private static final long serialVersionUID = 1L;
  long setupTime;
  long crunchNumberTime;
  long collectResultTime;
  GLBResultGR<T> rootGlbR;
  GLBParametersGR glbPara;
  ConsolePrinter consolePrinter;
  private int p = places().size();
  private GlobalRef<WorkerGR<Queue, T>> globalRef;

  public GLBCoopGR(SerializableCallable<Queue> init, GLBParametersGR glbPara, boolean tree) {
    consolePrinter = ConsolePrinter.getInstance();
    consolePrinter.println("[GLBCoopGR " + here() + "]: entering constructor.");
    this.glbPara = glbPara;
    this.setupTime = System.nanoTime();

    SerializableCallable<WorkerGR<Queue, T>> workerInit =
        () ->
            new WorkerGR<Queue, T>(
                init,
                glbPara.n,
                glbPara.w,
                glbPara.l,
                glbPara.z,
                glbPara.m,
                tree,
                glbPara.timestamps,
                glbPara.P);

    globalRef = new GlobalRef<>(places(), workerInit);

    final long l = System.nanoTime();
    this.setupTime = l - this.setupTime;

    for (Place p : places()) {
      at(
          p,
          () -> {
            globalRef.get().loggerGR.timeReference = l;
            globalRef.get().loggerGR.startStoppingTimeWithAutomaticEnd(LoggerGR.IDLING);
          });
    }

    consolePrinter.println("[Cooperative.GLBCoopGR " + here() + "]: leaving constructor.");
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
    consolePrinter.println("[Cooperative.GLBCoopGR " + here() + "]: starting with run().");
    crunchNumberTime = System.nanoTime();
    globalRef.get().main(globalRef, start);
    long now = System.nanoTime();
    crunchNumberTime = now - crunchNumberTime;
    consolePrinter.println("[Cooperative.GLBCoopGR " + here() + "]: reducing result.");
    T[] r = collectResults();
    end(r);
    return r;
  }

  /**
   * Run method. This method is called when users can know the workload upfront and initialize the
   * workload in {@link TaskQueueGR}
   *
   * @return {@link #collectResults()}
   */
  public T[] runParallel() {
    consolePrinter.println("[Cooperative.GLBCoopGR " + here() + "]: starting with runParallel().");
    crunchNumberTime = System.nanoTime();
    WorkerGR.broadcast(globalRef);
    long now = System.nanoTime();
    crunchNumberTime = now - crunchNumberTime;
    consolePrinter.println("[Cooperative.GLBCoopGR " + here() + "]: reducing result.");
    T[] r = collectResults();
    //        T[] r = this.globalRef.get().queue.getResult().getResult();
    end(r);
    return r;
  }

  /**
   * Print various Cooperative.GLB-related information, including result; time spent in
   * initialization, computation and result collection; any user specified log information (per
   * place); and Cooperative.GLB statistics.
   *
   * @param r result to println
   */
  private void end(T[] r) {
    // println result
    if (0 != (glbPara.v & GLBParametersGR.SHOW_RESULT_FLAG)) {
      rootGlbR.display(r);
    }
    // println overall timing information
    if (0 != (glbPara.v & GLBParametersGR.SHOW_TIMING_FLAG)) {
      System.out.println("Setup time:" + ((setupTime) / 1E9));
      System.out.println("Process time:" + ((crunchNumberTime) / 1E9));
      System.out.println("Result reduce time:" + (collectResultTime / 1E9));
    }

    // println log
    if (0 != (glbPara.v & GLBParametersGR.SHOW_TASKFRAME_LOG_FLAG)) {
      //      printLog();
    }

    // collect glb statistics and println it out
    if (0 != (glbPara.v & GLBParametersGR.SHOW_GLB_FLAG)) {
      collectLifelineStatus(globalRef);
    }
  }

  /**
   * Collect Cooperative.GLB statistics
   *
   * @param globalRef PlaceLocalHandle for {@link WorkerGR}
   */
  private void collectLifelineStatus(GlobalRef<WorkerGR<Queue, T>> globalRef) {
    final GlobalRef<LoggerGR[]> logs = new GlobalRef<>(new LoggerGR[p]);

    finish(
        () -> {
          for (Place p : places()) {
            asyncAt(
                p,
                () -> {
                  globalRef.get().loggerGR.stoppingTimeToResult();
                  final LoggerGR logRemote = globalRef.get().loggerGR.get();
                  final int idRemote = here().id;
                  asyncAt(
                      logs.home(),
                      () -> {
                        logs.get()[idRemote] = logRemote;
                      });
                });
          }
        });

    for (final LoggerGR l : logs.get()) {
      System.out.println(l);
    }

    LoggerGR log = new LoggerGR(glbPara.timestamps);
    log.collect(logs.get());
    log.stats();

    try {
      log.printStoppedTime();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  protected T[] collectResults() {
    this.collectResultTime = System.nanoTime();

    GlobalRef<TaskQueueGR[]> globalResults = new GlobalRef<>(new TaskQueueGR[places().size()]);
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

    TaskQueueGR result = null;
    for (final TaskQueueGR q : globalResults.get()) {
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
   * @param globalRef GlobalRef for {@link WorkerGR}
   */
  private void printLog(GlobalRef<WorkerGR<Queue, T>> globalRef) {
    int P = places().size();
    finish(
        () -> {
          for (int i = 0; i < P; ++i) {
            asyncAt(place(i), () -> globalRef.get().queue.printLog());
          }
        });
  }
}
