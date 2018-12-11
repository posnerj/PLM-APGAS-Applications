package examples.bc;

import static apgas.Constructs.asyncAny;
import static apgas.Constructs.enableStaticDistribution;
import static apgas.Constructs.finishAsyncAny;
import static apgas.Constructs.places;
import static apgas.Constructs.reduceAsyncAny;
import static apgas.Constructs.staticAsyncAny;
import static apgas.Constructs.staticInit;

import apgas.Configuration;
import apgas.Place;
import apgas.SerializableJob;
import apgas.impl.Config;
import java.util.ArrayList;
import java.util.Calendar;
import utils.Rmat;

/** Created by jposner on 15.03.17. */
public class StartBC {

  public static void main(String[] args) {
    System.out.println("Start date: " + Calendar.getInstance().getTime());

    if (System.getProperty(Configuration.APGAS_PLACES) == null) {
      System.setProperty(Configuration.APGAS_PLACES, "4");
    }
    if (System.getProperty(Configuration.APGAS_THREADS) == null) {
      System.setProperty(Configuration.APGAS_THREADS, "4");
    }
    System.setProperty(Config.APGAS_SERIALIZATION, "java");

    final int numThreads = Integer.parseInt(System.getProperty(Configuration.APGAS_THREADS));

    int seed = 2;
    int n = 13;
    double a = 0.55;
    double b = 0.1;
    double c = 0.1;
    double d = 0.25;
    int permute = 1;

    if (args.length == 1) {
      n = Integer.parseInt(args[0]);
    }

    System.out.println(
        "Running "
            + StartBC.class.getName()
            + " with "
            + places().size()
            + " places and "
            + numThreads
            + " Threads with "
            + "the following parameters:");
    System.out.println("seed = " + seed);
    System.out.println("N = " + (1 << n));
    System.out.println("a = " + a);
    System.out.println("b = " + b);
    System.out.println("c = " + c);
    System.out.println("d = " + d);
    System.out.println("Seq ");

    final int N = 1 << n;

    long start;
    long end;

    start = System.nanoTime();
    final Rmat rmat = new Rmat(seed, n, a, b, c, d);
    end = System.nanoTime();
    System.out.println("Rmat time: " + ((end - start) / 1E9D));
    start = System.nanoTime();

    final int maxThreads = Integer.getInteger(Config.APGAS_MAX_THREADS, 256);
    System.out.println("maxThreads: " + maxThreads);

    staticInit(
        () -> {
          BC.initGraph(rmat, permute, maxThreads);
        });
    enableStaticDistribution();

    end = System.nanoTime();
    System.out.println("staticInit time: " + ((end - start) / 1E9D));
    start = System.nanoTime();

    final int bagSize = 32;
    final int stepSize = (int) Math.ceil((double) N / (double) places().size());

    if (places().size() == 1) {
      finishAsyncAny(
          () -> {
            for (int i = 0; i < N; i++) {
              final int from = i;
              i += bagSize;
              final int to = Math.min(i, N);
              asyncAny(
                  () -> {
                    BC.compute(from, to);
                  });
              i--;
            }
          });
    } else {
      finishAsyncAny(
          () -> {
            for (Place p : places()) {
              final int currentID = p.id;
              int from = currentID * stepSize;
              int to = Math.min(N, (currentID + 1) * stepSize);

              ArrayList<SerializableJob> list = new ArrayList<>();
              for (int i = from; i < to; i++) {

                final int begin = i;
                final int until = Math.min(to, begin + bagSize);

                list.add(
                    () -> {
                      BC.compute(begin, until);
                    });
                i += (bagSize - 1);
              }
              staticAsyncAny(p, list);
            }
          });
    }

    end = System.nanoTime();
    System.out.println("Process time: " + ((end - start) / 1E9D));
    start = System.nanoTime();
    reduceAsyncAny().display();
    end = System.nanoTime();
    System.out.println("Reduce time: " + ((end - start) / 1E9D));
    System.out.println("End date: " + Calendar.getInstance().getTime());
  }
}
