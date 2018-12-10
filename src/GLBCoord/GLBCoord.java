package GLBCoord;

import static apgas.Constructs.async;
import static apgas.Constructs.asyncAt;
import static apgas.Constructs.at;
import static apgas.Constructs.finish;
import static apgas.Constructs.here;
import static apgas.Constructs.place;
import static apgas.Constructs.places;

import GLBCoop.GLBParameters;
import GLBCoop.GLBResult;
import GLBCoop.Logger;
import GLBCoop.TaskQueue;
import GLBCoopGR.WorkerGR;
import apgas.DeadPlaceException;
import apgas.Place;
import apgas.SerializableCallable;
import apgas.util.GlobalRef;
import java.io.IOException;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import java.util.function.Function;
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

    for (Place p : places()) {
      at(
          p,
          () -> {
            globalRef.get().logger.timeReference = l;
            globalRef.get().logger.startStoppingTimeWithAutomaticEnd(Logger.DEAD);
          });
    }

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
   * @return {@link #collectResults(long)}
   */
  public T[] run(Runnable start) {
    consolePrinter.println("[GLBCoord.SplitGLB " + here() + "]: starting with run().");
    crunchNumberTime = System.nanoTime();
    globalRef.get().main(globalRef, start);
    long now = System.nanoTime();
    crunchNumberTime = now - crunchNumberTime;
    consolePrinter.println("[GLBCoord.SplitGLB " + here() + "]: reducing result.");
    T[] r = collectResults(now);
    end(r);
    return r;
  }

  /**
   * Run method. This method is called when users can know the workload upfront and initialize the
   * workload in {@link TaskQueue}
   *
   * @return {@link #collectResults(long)}
   */
  public T[] runParallel() {
    consolePrinter.println("[GLBCoord.SplitGLB " + here() + "]: starting with runParallel().");
    crunchNumberTime = System.nanoTime();
    WorkerCoord.broadcast(globalRef);
    long now = System.nanoTime();
    crunchNumberTime = now - crunchNumberTime;
    consolePrinter.println("[GLBCoord.SplitGLB " + here() + "]: reducing result.");
    T[] r = collectResults(now);
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
    // println overall timing information
    if (0 != (glbPara.v & GLBParameters.SHOW_TIMING_FLAG)) {
      System.out.println("Setup time(timestamps):" + ((setupTime) / 1E9));
      System.out.println("Process time(timestamps):" + ((crunchNumberTime) / 1E9));
      System.out.println("Result reduce time(timestamps):" + (collectResultTime / 1E9));
    }

    // println log
    if (0 != (glbPara.v & GLBParameters.SHOW_TASKFRAME_LOG_FLAG)) {
//      printLog(globalRef);
    }

    Logger l = null;
    // collect glb statistics and println it out
    if (0 != (glbPara.v & GLBParameters.SHOW_GLB_FLAG)) {
      l = collectLifelineStatus(globalRef);
    }

    long totalCountRelease = 0;
    long totalCountReacquire = 0;

    for (Place p : places()) {
      totalCountReacquire +=
          at(
              p,
              () -> {
                return globalRef.get().queue.getCountRequire();
              });
      totalCountRelease +=
          at(
              p,
              () -> {
                return globalRef.get().queue.getCountRelease();
              });
    }

    if (l != null) {
      double stealRatio =
          (double) l.nodesGiven / (double) l.stealsPerpetrated
              + (double) l.lifelineStealsPerpetrated;
      double accRatio = (double) totalCountReacquire / stealRatio;

      DecimalFormatSymbols otherSymbols = new DecimalFormatSymbols(Locale.GERMAN);
      otherSymbols.setDecimalSeparator('.');
      otherSymbols.setGroupingSeparator('.');
      DecimalFormat df = new DecimalFormat("#.##", otherSymbols);

      System.out.println(
          "countRelease:"
              + totalCountRelease
              + ", countReacquire:"
              + totalCountReacquire
              + ", Task items stolen:"
              + l.nodesGiven
              + ", (direct):"
              + l.nodesReceived
              + ", (lifeline):"
              + l.lifelineNodesReceived
              + ", successful direct steals:"
              + l.stealsPerpetrated
              + ", successful lifeline steals:"
              + l.lifelineStealsPerpetrated
              + ", Process time:"
              + ((crunchNumberTime) / 1E9)
              + ", Tasks per Steal:"
              + df.format(stealRatio)
              + ", accRatio: "
              + df.format(accRatio));
    }
  }

  /**
   * Collect Cooperative.FTGLB statistics
   *
   * @param globalRef PlaceLocalHandle for {@link WorkerCoord}
   */
  private Logger collectLifelineStatus(GlobalRef<WorkerCoord<Queue, T>> globalRef) {
    Logger[] logs;
    // val groupSize:Long = 128;
    //        boolean params = (0 != (this.glbPara.v & GLBParameters.SHOW_GLB_FLAG));
    final int V = this.glbPara.v;
    final int P = p;
    final int S = this.glbPara.timestamps;

    if (1024 < p) {
      Function<Integer, Logger> filling =
          (Function<Integer, Logger> & Serializable)
              (Integer i) ->
                  at(
                      places().get(i * 32),
                      () -> {
                        final int h = here().id;
                        final int n = Math.min(32, P - h);

                        Function<Integer, Logger> newFilling =
                            (Function<Integer, Logger> & Serializable)
                                (j ->
                                    at(
                                        places().get(h + j),
                                        () ->
                                            globalRef
                                                .get()
                                                .logger
                                                .get((V & GLBParameters.SHOW_GLB_FLAG) != 0)));

                        Logger[] newLogs = fillLogger(new Logger[n], newFilling);
                        Logger newLog = new Logger(S);
                        newLog.collect(newLogs);
                        return newLog;
                      });
      logs = fillLogger(new Logger[p / 32], filling);
    } else {
      Function<Integer, Logger> newFilling =
          (Function<Integer, Logger> & Serializable)
              i ->
                  at(
                      places().get(i),
                      () -> globalRef.get().logger.get((V & GLBParameters.SHOW_GLB_FLAG) != 0));

      logs = fillLogger(new Logger[p], newFilling);
    }

    Logger log = new Logger(glbPara.timestamps);
    log.collect(logs);
    log.stats();

    try {
      log.printStoppedTime();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return log;
  }

  protected T[] collectResults(long now) {
    this.collectResultTime = System.nanoTime();
    this.rootGlbR = this.globalRef.get().queue.getResult();

    finish(
        () -> {
          for (final Place p : places()) {
            if (here().id == p.id) {
              consolePrinter.println(
                  here() + "(in collectResults): count_1 = " + +globalRef.get().queue.count());
              consolePrinter.println(
                  here() + "(in collectResults): size_1 = " + globalRef.get().queue.size());
              continue;
            }
            try {
              TaskQueueCoord<Queue, T> q = at(p, () -> globalRef.get().queue);
              consolePrinter.println(
                  at(
                      p,
                      () ->
                          here()
                              + "(in collectResults): count_2 = "
                              + +globalRef.get().queue.count()));
              consolePrinter.println(
                  at(
                      p,
                      () ->
                          here()
                              + "(in collectResults): size_2 = "
                              + globalRef.get().queue.size()));
              globalRef.get().queue.mergeResult(q);
            } catch (final DeadPlaceException e) {
              async(
                  () -> {
                    throw e;
                  });
            }
          }
        });
    collectResultTime = System.nanoTime() - collectResultTime;
    return globalRef.get().queue.getResult().getResult();
  }

  /**
   * Print logging information on each place if user is interested in collecting per place
   * information, i.e., statistics instrumented.
   *
   * @param globalRef GlobalRef for {@link WorkerCoord}
   */
  private void printLog(GlobalRef<WorkerCoord<Queue, T>> globalRef) {
    int P = places().size();
    finish(() -> {
      for (int i = 0; i < P; ++i) {
        asyncAt(place(i), () -> globalRef.get().queue.printLog());
      }
    });
  }


  private Logger[] fillLogger(Logger[] arr, Function<Integer, Logger> function) {
    long now = System.nanoTime();
    for (int i = 0; i < arr.length; i++) {
      arr[i] = function.apply(i);
      final long l = System.nanoTime();
      consolePrinter.println("" + (l - now) / 1E9);
      now = l;
    }
    return arr;
  }
}
