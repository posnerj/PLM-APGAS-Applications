package GLBCoop;

import static apgas.Constructs.async;
import static apgas.Constructs.asyncAt;
import static apgas.Constructs.at;
import static apgas.Constructs.finish;
import static apgas.Constructs.here;
import static apgas.Constructs.place;
import static apgas.Constructs.places;

import apgas.DeadPlaceException;
import apgas.Place;
import apgas.SerializableCallable;
import apgas.util.GlobalRef;
import java.io.IOException;
import java.io.Serializable;
import java.util.function.Function;
import utils.ConsolePrinter;

public class GLBCoop<Queue extends TaskQueue<Queue, T>, T extends Serializable>
    implements Serializable {

  private static final long serialVersionUID = 1L;
  long setupTime;
  long crunchNumberTime;
  long collectResultTime;
  GLBResult<T> rootGlbR;
  GLBParameters glbPara;
  ConsolePrinter consolePrinter;
  private int p = places().size();
  private GlobalRef<Worker<Queue, T>> globalRef;

  public GLBCoop(SerializableCallable<Queue> init, GLBParameters glbPara, boolean tree) {
    consolePrinter = ConsolePrinter.getInstance();
    consolePrinter.println("[GLBCoop " + here() + "]: entering constructor.");
    this.glbPara = glbPara;
    this.setupTime = System.nanoTime();

    SerializableCallable<Worker<Queue, T>> workerInit =
        () ->
            new Worker<Queue, T>(
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
            globalRef.get().logger.timeReference = l;
            globalRef.get().logger.startStoppingTimeWithAutomaticEnd(Logger.IDLING);
          });
    }

    consolePrinter.println("[Cooperative.GLBCoop " + here() + "]: leaving constructor.");
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
    consolePrinter.println("[Cooperative.GLBCoop " + here() + "]: starting with run().");
    crunchNumberTime = System.nanoTime();
    globalRef.get().main(globalRef, start);
    long now = System.nanoTime();
    crunchNumberTime = now - crunchNumberTime;
    consolePrinter.println("[Cooperative.GLBCoop " + here() + "]: reducing result.");
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
    consolePrinter.println("[Cooperative.GLBCoop " + here() + "]: starting with runParallel().");
    crunchNumberTime = System.nanoTime();
    Worker.broadcast(globalRef);
    long now = System.nanoTime();
    crunchNumberTime = now - crunchNumberTime;
    consolePrinter.println("[Cooperative.GLBCoop " + here() + "]: reducing result.");
    T[] r = collectResults(now);
    //        T[] r = this.globalRef.get().queue.getResult().getResult();
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
      System.out.println("Setup time:" + ((setupTime) / 1E9));
      System.out.println("Process time:" + ((crunchNumberTime) / 1E9));
      System.out.println("Result reduce time:" + (collectResultTime / 1E9));
    }

    // println log
    if (0 != (glbPara.v & GLBParameters.SHOW_TASKFRAME_LOG_FLAG)) {
      printLog(globalRef);
    }

    // collect glb statistics and println it out
    if (0 != (glbPara.v & GLBParameters.SHOW_GLB_FLAG)) {
      collectLifelineStatus(globalRef);
    }
  }

  /**
   * Collect Cooperative.FTGLB statistics
   *
   * @param globalRef PlaceLocalHandle for {@link Worker}
   */
  private void collectLifelineStatus(GlobalRef<Worker<Queue, T>> globalRef) {
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
      //            Function<Integer, Logger> newFilling = (Function<Integer, Logger> &
      // Serializable) i -> at(place(i),
      //                    () -> {
      //                        return worker.logger.get((V & FTGLBParameters.SHOW_GLB_FLAG) != 0);
      //                    });

      logs = new Logger[p];
      finish(
          () -> {
            for (Place p : places()) {
              asyncAt(
                  p,
                  () -> {
                    globalRef.get().logger.stoppingTimeToResult();
                  });
            }
          });

      for (int i = 0; i < p; i++) {
        logs[i] = at(place(i), () -> globalRef.get().logger.get(true));
        //                System.out.println("FTGLB, logs[" + i + "].stoppingResult.length = " +
        // logs[i].stoppingResult.length);
      }
      //            logs = fillLogger(new FTLogger[p], newFilling);
    }

    Logger log = new Logger(glbPara.timestamps);
    log.collect(logs);
    log.stats();

    try {
      log.printStoppedTime();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  protected T[] collectResults(long now) {
    final GlobalRef<Worker<Queue, T>> globalRef = this.globalRef;
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
              TaskQueue<Queue, T> q =
                  at(
                      p,
                      () -> {
                        return globalRef.get().queue;
                      });

              consolePrinter.println(
                  at(
                      p,
                      () ->
                          here()
                              + "(in collectResults): count_2 = "
                              + globalRef.get().queue.count()));
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
   * @param globalRef GlobalRef for {@link Worker}
   */
  private void printLog(GlobalRef<Worker<Queue, T>> globalRef) {
    int P = places().size();
    for (int i = 0; i < P; ++i) {
      at(places().get(i), () -> globalRef.get().queue.printLog());
    }
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
