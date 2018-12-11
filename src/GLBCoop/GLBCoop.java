package GLBCoop;

import static apgas.Constructs.asyncAt;
import static apgas.Constructs.at;
import static apgas.Constructs.finish;
import static apgas.Constructs.here;
import static apgas.Constructs.place;
import static apgas.Constructs.places;

import apgas.Place;
import apgas.SerializableCallable;
import apgas.util.GlobalRef;
import apgas.util.PlaceLocalObject;
import java.io.IOException;
import java.io.Serializable;
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
  Worker<Queue, T> worker;

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

    this.worker = PlaceLocalObject.make(places(), workerInit);

    final long l = System.nanoTime();
    this.setupTime = l - this.setupTime;

    for (Place p : places()) {
      at(p, () -> {
        worker.logger.timeReference = l; //TODO hier oder ueber dem for?
        worker.logger.startStoppingTimeWithAutomaticEnd(Logger.IDLING);
      });
    }

    consolePrinter.println("[Cooperative.GLBCoop " + here() + "]: leaving constructor.");
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
    consolePrinter.println("[Cooperative.GLBCoop " + here() + "]: starting with run().");
    crunchNumberTime = System.nanoTime();
    worker.main(start);
    long now = System.nanoTime();
    crunchNumberTime = now - crunchNumberTime;
    consolePrinter.println("[Cooperative.GLBCoop " + here() + "]: reducing result.");
    T[] r = collectResults();
    end(r);
    return r;
  }

  /**
   * Run method. This method is called when users can know the workload upfront and initialize the
   * workload in {@link TaskQueue}
   *
   * @return {@link #collectResults()}
   */
  public T[] runParallel() {
    consolePrinter.println("[Cooperative.GLBCoop " + here() + "]: starting with runParallel().");
    crunchNumberTime = System.nanoTime();
    Worker.broadcast(worker);
    long now = System.nanoTime();
    crunchNumberTime = now - crunchNumberTime;
    consolePrinter.println("[Cooperative.GLBCoop " + here() + "]: reducing result.");
    T[] r = collectResults();
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
      collectLifelineStatus();
    }
  }

  /**
   * Collect Cooperative.GLB statistics
   */
  private void collectLifelineStatus() {
    final GlobalRef<Logger[]> logs = new GlobalRef<>(new Logger[p]);

    finish(() -> {
      for (Place p : places()) {
        asyncAt(p, () -> {
          worker.logger.stoppingTimeToResult();
          final Logger logRemote = worker.logger.get();
          final int idRemote = here().id;
          asyncAt(logs.home(), () -> {
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
    log.stats();

    try {
      log.printStoppedTime();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  protected T[] collectResults() {
    this.collectResultTime = System.nanoTime();

    GlobalRef<TaskQueue[]> globalResults = new GlobalRef<>(new TaskQueue[places().size()]);
    finish(() -> {
      for (final Place p : places()) {
        asyncAt(p, () -> {
          final Queue qRemote = worker.queue;
          final int idRemote = here().id;
          asyncAt(globalResults.home(), () -> {
            globalResults.get()[idRemote] = qRemote;
          });
        });
      }
    });

    TaskQueue result = null;
    for (final TaskQueue q : globalResults.get()) {
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
   */
  private void printLog() {
    int P = places().size();
    finish(() -> {
      for (int i = 0; i < P; ++i) {
        asyncAt(place(i), () -> worker.queue.printLog());
      }
    });
  }
}
