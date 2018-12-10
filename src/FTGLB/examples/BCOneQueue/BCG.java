package FTGLB.examples.BCOneQueue;

import static apgas.Constructs.places;

import FTGLB.FTGLB;
import FTGLB.FTGLBParameters;
import apgas.Configuration;
import apgas.SerializableCallable;
import apgas.impl.Config;
import java.util.Calendar;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import utils.Rmat;

public class BCG {

  static final int COUNT_PLACES = 4;
  static final int COUNT_THREADS = 16;

  private static Double[] compute(String[] args) throws ParseException {
    Options options = new Options();
    options.addOption("s", true, "Seed for the random number");
    options.addOption("n", true, "Number of vertices = 2^n");
    options.addOption("a", true, "Probability a");
    options.addOption("b", true, "Probability b");
    options.addOption("c", true, "Probability c");
    options.addOption("d", true, "Probability d");
    options.addOption("p", true, "Permutation");
    options.addOption("g", true, "Number of nodes to process before probing. Default 511.");
    options.addOption("w", true, "Number of thieves to send out Default 1.");
    options.addOption("l", true, "Base of the lifeline");
    options.addOption("m", true, "Max potential victims");
    options.addOption("k", true, "Backup-Cycles");
    options.addOption("v", true, "Verbose");
    options.addOption(
        "timestamps",
        true,
        "count of timestamps for logging, 0=disabled, 500 = recommanded, default is 0");
    options.addOption("crashNumber", true, "postion of crashing, default is 0");
    options.addOption("backupCount", true, "count of backups, default is 1");

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    int seed = Integer.parseInt(cmd.getOptionValue("s", "2"));
    int n = Integer.parseInt(cmd.getOptionValue("n", "14")); // 2
    double a = Double.parseDouble(cmd.getOptionValue("a", "0.55"));
    double b = Double.parseDouble(cmd.getOptionValue("b", "0.1"));
    double c = Double.parseDouble(cmd.getOptionValue("c", "0.1"));
    double d = Double.parseDouble(cmd.getOptionValue("d", "0.25"));
    int permute = Integer.parseInt(cmd.getOptionValue("p", "1"));
    int g = Integer.parseInt(cmd.getOptionValue("g", "127"));
    int l = Integer.parseInt(cmd.getOptionValue("l", "32"));
    int m = Integer.parseInt(cmd.getOptionValue("m", "1024"));
    int k = Integer.parseInt(cmd.getOptionValue("k", "4"));
    int crashNumber = Integer.parseInt(cmd.getOptionValue("crashNumber", "0"));
    int backupCount = Integer.parseInt(cmd.getOptionValue("backupCount", "1"));

    int timestamps = Integer.parseInt(cmd.getOptionValue("timestamps", "0"));

    if (System.getProperty(Configuration.APGAS_PLACES) == null) {
      System.setProperty(Configuration.APGAS_PLACES, String.valueOf(COUNT_PLACES));
    }

    String sysThreads = System.getProperty(Configuration.APGAS_THREADS);
    if (sysThreads == null || Integer.parseInt(sysThreads) < COUNT_THREADS) {
      System.setProperty(Configuration.APGAS_THREADS, String.valueOf(COUNT_THREADS));
    }
    sysThreads = System.getProperty(Configuration.APGAS_THREADS);

    System.setProperty(Config.APGAS_SERIALIZATION, "java");

    int verbose =
        Integer.parseInt(cmd.getOptionValue("v", String.valueOf(FTGLBParameters.SHOW_RESULT_FLAG)));

    int numPlaces = places().size();

    int z0 = 1;
    int zz = l;
    while (zz < numPlaces) {
      z0++;
      zz *= l;
    }

    int z = z0;
    int w = Integer.parseInt(cmd.getOptionValue("w", String.valueOf(z)));

    System.out.println(
        "places = "
            + numPlaces
            + "   w = "
            + w
            + "   g = "
            + g
            + "   l = "
            + l
            + "   m = "
            + m
            + "   z = "
            + z
            + "   v = "
            + verbose
            + "   timestamps = "
            + timestamps
            + "   sysThreads = "
            + sysThreads
            + "   k = "
            + k
            + "   crashNumber = "
            + crashNumber
            + "   backupCount = "
            + backupCount);

    if (backupCount > 6) {
      System.out.println("backupCount  can't be larger than than 6");
      return null;
    }

    Boolean propertyApgasRes = Boolean.valueOf(System.getProperty(Configuration.APGAS_RESILIENT));
    if (propertyApgasRes == null || propertyApgasRes.equals(false)) {
      System.out.println("Warning: APGAS_RESILIENT is disabled!!!!");
    }

    System.out.println(
        "seed = "
            + seed
            + ", N = "
            + (1 << n)
            + ", a = "
            + a
            + ", b = "
            + b
            + ", c = "
            + c
            + ", d = "
            + d
            + ", places = "
            + numPlaces);

    SerializableCallable<Queue> init =
        () -> new Queue(new Rmat(seed, n, a, b, c, d), permute, numPlaces);

    FTGLBParameters glbPara =
        new FTGLBParameters(
            g, w, l, z, m, verbose, timestamps, k, crashNumber, backupCount, numPlaces);
    FTGLB<Queue, Double> glb = new FTGLB<Queue, Double>(init, glbPara, false);

    Double[] result = glb.runParallel();
    return result;
  }

  public static void main(String[] args) {
    System.out.println("Start date: " + Calendar.getInstance().getTime());
    System.out.println(BCG.class.getName() + " starts");
    try {
      compute(args);
    } catch (ParseException e) {
      e.printStackTrace();
    }
    System.out.println("End date: " + Calendar.getInstance().getTime());
  }
}
