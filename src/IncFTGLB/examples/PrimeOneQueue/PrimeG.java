package IncFTGLB.examples.PrimeOneQueue;

import static IncFTGLB.IncFTGLBParameters.computeL;
import static IncFTGLB.IncFTGLBParameters.computeZ;
import static apgas.Constructs.places;

import IncFTGLB.IncFTGLB;
import IncFTGLB.IncFTGLBParameters;
import apgas.Configuration;
import apgas.SerializableCallable;
import java.util.Calendar;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class PrimeG {

  static final int COUNT_PLACES = 4;
  static final int COUNT_THREADS = 16;

  private static Integer[] compute(String[] args) throws ParseException {
    Options options = new Options();

    options.addOption("n", true, "Number of nodes to process before probing. Default 511.");
    options.addOption("f", true, "Lower bound of interval. Default 0.");
    options.addOption("t", true, "Upper bound of interval. Default 10^6.");
    options.addOption("w", true, "Number of thieves to send out. Default 1.");
    options.addOption("l", true, "Base of the lifeline");
    options.addOption("m", true, "Max potential victims");
    options.addOption("v", true, "Verbose. Default 0 (no).");
    options.addOption(
        "k", true, " Write cyclic backups every k * n computation-elements. Default: 2048");
    options.addOption(
        "timestamps",
        true,
        "count of timestamps for logging, 0=disabled, 500 = recommanded, default ist 0");
    options.addOption("crashNumber", true, "postion of crashing, default ist 0");
    options.addOption("backupCount", true, "count of backups, default ist 1");

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    int n = Integer.parseInt(cmd.getOptionValue("n", "255"));
    int f = Integer.parseInt(cmd.getOptionValue("f", "0"));
    int t = Integer.parseInt(cmd.getOptionValue("t", "1000000"));
    int m = Integer.parseInt(cmd.getOptionValue("m", "1024"));
    long k = Long.parseLong(cmd.getOptionValue("k", "16"));
    int timestamps = Integer.parseInt(cmd.getOptionValue("timestamps", "0"));
    int crashNumber = Integer.parseInt(cmd.getOptionValue("crashNumber", "0"));
    int backupCount = Integer.parseInt(cmd.getOptionValue("backupCount", "1"));

    if (System.getProperty(Configuration.APGAS_PLACES) == null) {
      System.setProperty(Configuration.APGAS_PLACES, String.valueOf(COUNT_PLACES));
    }

    String sysThreads = System.getProperty(Configuration.APGAS_THREADS);
    if (sysThreads == null || Integer.parseInt(sysThreads) < COUNT_THREADS) {
      System.setProperty(Configuration.APGAS_THREADS, String.valueOf(COUNT_THREADS));
    }
    sysThreads = System.getProperty(Configuration.APGAS_THREADS);

    int verbose =
        Integer.parseInt(
            cmd.getOptionValue("v", String.valueOf(IncFTGLBParameters.SHOW_RESULT_FLAG)));
    int numPlaces = places().size();

    int l = Integer.parseInt(cmd.getOptionValue("l", String.valueOf(computeL(numPlaces))));
    int z = computeZ(l, numPlaces);
    int w = Integer.parseInt(cmd.getOptionValue("w", String.valueOf(z)));

    System.out.println(
        "places = "
            + numPlaces
            + "   f = "
            + f
            + "   t = "
            + t
            + "   w = "
            + w
            + "   l = "
            + l
            + "   m = "
            + m
            + "   z = "
            + z
            + "   k = "
            + k
            + "   v = "
            + verbose
            + "   timestamps = "
            + timestamps
            + "   sysThreads = "
            + sysThreads
            + "   crashNumber = "
            + crashNumber
            + "   backupCount = "
            + backupCount);

    if (backupCount > 6) {
      System.out.println("backupCount  can't be larger than than 6");
      return null;
    }

    Boolean propertyApgasRes = Boolean.valueOf(System.getProperty(Configuration.APGAS_RESILIENT));
    System.out.println(propertyApgasRes);
    if (propertyApgasRes == null || propertyApgasRes.equals(false)) {
      System.out.println("Warning: APGAS_RESILIENT is disabled!!!!");
    }

    SerializableCallable<Queue> init = () -> new Queue(f, t, numPlaces);
    IncFTGLBParameters glbPara =
        new IncFTGLBParameters(
            n, w, l, z, m, verbose, timestamps, k, crashNumber, backupCount, numPlaces);
    IncFTGLB<Queue, Integer> glb = new IncFTGLB<Queue, Integer>(init, glbPara, false);

    Integer[] result = glb.runParallel();
    return result;
  }

  public static void main(String[] args) throws ParseException {
    int n = Integer.getInteger(utils.Constants.benchmarkIterations, 1);
    System.out.println("benchmarkIterations: " + n);
    for (int i = 0; i < n; i++) {
      System.out.println("Iteration: " + i + ", start date: " + Calendar.getInstance().getTime());
      System.out.println(PrimeG.class.getName() + " starts");
      Integer[] result = compute(args);
      System.out.println("Result: " + result[0]);

      if (i != (n - 1)) {
        System.out.println("Result of run " + i + " is: " + result[0]);
        System.out.println("Iteration: " + i + ", end date: " + Calendar.getInstance().getTime());
        System.out.println("\n\n\n---------------------------------------------------------\n\n\n");
      }
    }
  }
}
