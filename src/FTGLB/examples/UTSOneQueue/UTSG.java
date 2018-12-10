package FTGLB.examples.UTSOneQueue;

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

public class UTSG {

  static final int COUNT_PLACES = 4;
  static final int COUNT_THREADS = 16;

  public static Long[] compute(String[] args) throws ParseException {
    Options options = new Options();

    options.addOption("b", true, "Branching factor");
    options.addOption("r", true, "Seed (0 <= r < 2^31)");
    options.addOption("d", true, "Tree depth");
    options.addOption("n", true, "Number of nodes to process before probing. Default 511.");
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

    int b = Integer.parseInt(cmd.getOptionValue("b", "4"));
    int r = Integer.parseInt(cmd.getOptionValue("r", "19"));
    int d = Integer.parseInt(cmd.getOptionValue("d", "13"));
    int n = Integer.parseInt(cmd.getOptionValue("n", "511"));
    int l = Integer.parseInt(cmd.getOptionValue("l", "32"));
    int m = Integer.parseInt(cmd.getOptionValue("m", "1024"));
    int k = Integer.parseInt(cmd.getOptionValue("k", "2048"));
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

    System.setProperty(Config.APGAS_SERIALIZATION, "java");

    int verbose =
        Integer.parseInt(cmd.getOptionValue("v", String.valueOf(FTGLBParameters.SHOW_RESULT_FLAG)));
    int numPlaces = places().size();

    int z0 = 1;
    int zz = l;
    while (zz < numPlaces) {
      z0++;
      zz *= l;
      System.out.println("calculating zz...");
    }

    int z = z0;
    int w = Integer.parseInt(cmd.getOptionValue("w", String.valueOf(z)));

    System.out.println(
        "places = "
            + numPlaces
            + "   b = "
            + b
            + "   r = "
            + r
            + "   d = "
            + d
            + "   w = "
            + w
            + "   n = "
            + n
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
    if (propertyApgasRes == null || propertyApgasRes.equals(false)) {
      System.out.println("Warning: APGAS_RESILIENT is disabled!!!!");
    }

    SerializableCallable<Queue> init = () -> new Queue(b, numPlaces);
    FTGLBParameters glbPara =
        new FTGLBParameters(
            n, w, l, z, m, verbose, timestamps, k, crashNumber, backupCount, numPlaces);
    FTGLB<Queue, Long> glb = new FTGLB<Queue, Long>(init, glbPara, true);

    Runnable start = () -> glb.getTaskQueue().init(r, d);

    Long[] result = glb.run(start);
    return result;
  }

  public static void main(String[] args) {
    System.out.println("Start date: " + Calendar.getInstance().getTime());
    System.out.println(UTSG.class.getName() + " starts");
    Long[] result = new Long[0];
    try {
      result = compute(args);
    } catch (ParseException e) {
      e.printStackTrace();
    }

    System.out.println("Result of run is: " + result[0]);
    System.out.println("End date: " + Calendar.getInstance().getTime());
  }
}
