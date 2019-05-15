/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package examples.pi;

import static apgas.Constructs.*;

import apgas.Configuration;
import apgas.impl.Config;
import apgas.impl.Worker;
import java.util.Calendar;

/** Created by jonas on 02.04.17. */
public class StartPi {

  private static final boolean CANCEL = true;

  public static void main(String[] args) {
    System.out.println("Start date: " + Calendar.getInstance().getTime());

    if (System.getProperty(Configuration.APGAS_PLACES) == null) {
      System.setProperty(Configuration.APGAS_PLACES, "2");
    }

    if (System.getProperty(Configuration.APGAS_THREADS) == null) {
      System.setProperty(Configuration.APGAS_THREADS, "2");
    }
    System.setProperty(Config.APGAS_SERIALIZATION, "java");

    int numThreads = Integer.parseInt(System.getProperty(Configuration.APGAS_THREADS));

    final int N;
    if (args.length >= 1) {
      N = Integer.parseInt(args[0]);
    } else {
      N = 10000000;
    }

    System.out.println(
        "Running "
            + StartPi.class.getName()
            + " with "
            + places().size()
            + " places and "
            + numThreads
            + " "
            + "Threads, N="
            + N
            + ", "
            + (CANCEL ? "with" : "without")
            + " cancel");

    long start = System.nanoTime();

    if (false == CANCEL) {

      finishAsyncAny(
          () -> {
            final double deltaX = 1.0 / N;
            for (int i = 0; i < N; i++) {
              final int _i = i;
              asyncAny(
                  () -> {
                    double x = (_i + 0.5) * deltaX;
                    double r = (4.0 / (1 + x * x)) * deltaX;
                    mergeAsyncAny(new PiResult(r));
                  });
            }
          });
    } else { // true == CANCEL

      finishAsyncAny(
          () -> {
            final double deltaX = 1.0 / N;
            for (int i = 0; i < N; i++) {
              final int _i = i;
              cancelableAsyncAny(
                  () -> {
                    double x = (_i + 0.5) * deltaX;
                    double r = (4.0 / (1 + x * x)) * deltaX;
                    mergeAsyncAny(new PiResult(r));

                    if (here().id != 0 || ((Worker) Thread.currentThread()).getMyID() != 0) {
                      return;
                    }

                    Double partialResult = ((PiResult) reduceAsyncAny()).getResult();
                    if (partialResult > 3 && partialResult < 4) {
                      System.out.println("found limit, cancel now");
                      cancelAllCancelableAsyncAny();
                    }
                  });
            }
          });
    }

    long end = System.nanoTime();
    System.out.println("Process time: " + ((end - start) / 1E9D));
    start = System.nanoTime();
    PiResult result = (PiResult) reduceAsyncAny();
    end = System.nanoTime();
    System.out.println("Reduce time: " + ((end - start) / 1E9D));
    System.out.println("Pi result(reduceAsyncAny)=" + result.getResult());
    System.out.println("Pi result(reducePartialAsyncAny)=" + reduceAsyncAny().getResult());
    System.out.println("End date: " + Calendar.getInstance().getTime());
  }
}
