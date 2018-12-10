package GLBCoord.examples.SplitUTS;

import static apgas.Constructs.places;

import GLBCoop.GLBParameters;
import GLBCoord.GLBCoord;
import apgas.Configuration;
import apgas.SerializableCallable;
import java.util.Calendar;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class UTSG {

  static final int COUNT_PLACES = 4;
  static final int COUNT_THREADS = 8;

  public static Long[] compute(String[] args) throws ParseException {

    Options options = new Options();

    options.addOption("b", true, "Branching factor");
    options.addOption("r", true, "Seed (0 <= r < 2^31)");
    options.addOption("d", true, "Tree depth");
    options.addOption("n", true, "Number of nodes to process before probing. Default 200.");
    options.addOption("w", true, "Number of thieves to send out. Default 1.");
    options.addOption("l", true, "Base of the lifeline");
    options.addOption("m", true, "Max potential victims");
    options.addOption("v", true, "Verbose. Default 0 (no).");
    options.addOption(
        "timestamps",
        true,
        "count of timestamps for logging, 0=disabled, 500 = recommended, default ist 0");
    options.addOption("s", true, "split for SplitQueue, range is between 0 and 10, default ist 5");
    options.addOption(
        "f", true, "factor for splitting in a steal, range is between 0 and 10, default ist 5");

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    int b = Integer.parseInt(cmd.getOptionValue("b", "4"));
    int r = Integer.parseInt(cmd.getOptionValue("r", "19"));
    int d = Integer.parseInt(cmd.getOptionValue("d", "13"));
    int n = Integer.parseInt(cmd.getOptionValue("n", "511"));
    int l = Integer.parseInt(cmd.getOptionValue("l", "32"));
    int m = Integer.parseInt(cmd.getOptionValue("m", "1024"));
    int timestamps = Integer.parseInt(cmd.getOptionValue("timestamps", "0"));
    double split = Double.parseDouble(cmd.getOptionValue("s", "5")) / 10;
    double steal = Double.parseDouble(cmd.getOptionValue("f", "5")) / 10;

    if (System.getProperty(Configuration.APGAS_PLACES) == null) {
      System.setProperty(Configuration.APGAS_PLACES, String.valueOf(COUNT_PLACES));
    }

    String sysThreads = System.getProperty(Configuration.APGAS_THREADS);
    if (sysThreads == null || Integer.parseInt(sysThreads) < COUNT_THREADS) {
      System.setProperty(Configuration.APGAS_THREADS, String.valueOf(COUNT_THREADS));
    }
    sysThreads = System.getProperty(Configuration.APGAS_THREADS);

    int verbose =
        Integer.parseInt(cmd.getOptionValue("v", String.valueOf(GLBParameters.SHOW_RESULT_FLAG)));

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
            + "   v = "
            + verbose
            + "   timestamps = "
            + timestamps
            + "   sysThreads = "
            + sysThreads
            + "   split = "
            + split
            + "   steal = "
            + steal);

    SerializableCallable<Queue> init = () -> new Queue(b, split, steal);
    GLBParameters glbPara = new GLBParameters(n, w, l, z, m, verbose, timestamps, numPlaces);
    GLBCoord<Queue, Long> glb = new GLBCoord<Queue, Long>(init, glbPara, true);

    Runnable start = () -> glb.getTaskQueue().init(r, d);

    Long[] result = glb.run(start);
    return result;
  }

  public static void main(String[] args) {
    System.out.println("Start date: " + Calendar.getInstance().getTime());
    System.out.println(GLBCoop.examples.UTS.UTSG.class.getName() + " starts");
    try {
      Long[] result = compute(args);
      System.out.println("Result of run is: " + result[0]);
    } catch (ParseException e) {
      e.printStackTrace();
    }
    System.out.println("End date: " + Calendar.getInstance().getTime());
  }
}
