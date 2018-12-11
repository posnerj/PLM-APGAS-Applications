package GLBCoop;

import static apgas.Constructs.here;
import static apgas.Constructs.places;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;
import utils.ConsolePrinter;

public class Logger implements Serializable {

  public static final int COMPUTING = 1;
  public static final int STEALING = 2;
  public static final int DISTRIBUTING = 3;
  public static final int DEAD = 4;

  public static final int PROCESSING = 1;
  public static final int COMMUNICATION = 2;
  public static final int WAITING = 3;
  public static final int IDLING = 4;

  private static final long serialVersionUID = 1L;
  private static final String FILENAME = "/home/users/0019/uk000618/workspace/gnuplot/dataCoop";

  private static final String FILEEND = ".csv";
  public long nodesCount = 0;
  public long nodesGiven = 0;
  public long lifelineNodesReceived = 0;
  /* (random)stealing requests stat*/
  public long stealsAttempted = 0;
  public long stealsPerpetrated = 0;
  public long stealsReceived = 0;
  public long stealsSuffered = 0;
  public long nodesReceived = 0;
  /* (lifeline)stealing requests stat*/
  public long lifelineStealsAttempted = 0;
  public long lifelineStealsPerpetrated = 0;
  public long lifelineStealsReceived = 0;
  public long lifelineStealsSuffered = 0;
  /* timing stat */
  public long lastStartStopLiveTimeStamp = -1;
  public long timeAlive = 0;
  public long timeDead = 0;
  public long startTime = 0;
  public long timeReference;
  public int id = 0;

  public int lastAutomaticEnd = 0;
  private final int placeID;

  ConsolePrinter consolePrinter;

  private ArrayList<Integer> timeTypes =
      new ArrayList<>(Arrays.asList(PROCESSING, COMMUNICATION, WAITING, IDLING));

  private int timestamps = 0;
  private ArrayList<Time> stoppingTime;
  private Double[][] stoppingResult;

  public Logger(int timestamps) {
    this.timeReference = System.nanoTime();
    this.placeID = here().id;
    if (timestamps > 0) {
      this.stoppingTime = new ArrayList<>();
      this.stoppingResult = new Double[timestamps][timeTypes.size() + 1];

      for (int i = 0; i < timestamps; i++) {
        for (int j = 0; j < timeTypes.size() + 1; j++) {
          this.stoppingResult[i][j] = 0.0d;
        }
      }
    }

    this.timestamps = timestamps;
    this.consolePrinter = ConsolePrinter.getInstance();
  }

  static String sub(String str, int start, int end) {
    return str.substring(start, Math.min(end, str.length()));
  }

  public synchronized void startStoppingTimeWithAutomaticEnd(int newProcess) {
    if (stoppingTime == null) {
      return;
    }

    if (timeTypes.contains(newProcess) == false) {
      throw new IllegalArgumentException();
    }

    long now = System.nanoTime();
    Time time = new Time(now, newProcess, true);
    stoppingTime.add(time);
    stoppingTime.get(lastAutomaticEnd).endTime = now;
    lastAutomaticEnd = time.id;
  }

  public synchronized void endStoppingTimeWithAutomaticEnd() {
    if (stoppingTime == null) {
      return;
    }
    stoppingTime.get(lastAutomaticEnd).endTime = System.nanoTime();
  }

  public synchronized int startStoppingTime(int newProcess) {
    if (stoppingTime == null) {
      return Integer.MIN_VALUE;
    }

    if (timeTypes.contains(newProcess) == false) {
      throw new IllegalArgumentException();
    }

    int myId = this.id;
    this.id++;
    if (myId == Integer.MAX_VALUE) {
      System.err.println("Logger:startStoppingTime: max value in id!!!!!");
    }
    stoppingTime.add(new Time(System.nanoTime(), newProcess, false, myId));
    return myId;
  }

  public synchronized void endStoppingTime(int newId) {
    if (stoppingTime == null) {
      return;
    }

    stoppingTime.get(newId).endTime = System.nanoTime();
  }

  public synchronized void printStoppedTime() throws IOException {
    if (stoppingResult == null) {
      if (here().id == 0) {
        System.out.println(
            "Logger:printStoppedTime(): stoppingTime was not startet, wrong Constructor called?");
      }
      return;
    }

    SimpleDateFormat formatter = new SimpleDateFormat("yyyy.MM.dd-HH:mm:ss");
    Date currentTime = new Date();
    String fullFileName =
        FILENAME + "_" + places().size() + "_" + formatter.format(currentTime) + FILEEND;

    BufferedWriter br = new BufferedWriter(new FileWriter(fullFileName));

    br.write("PROCESSING; COMMUNICATION; WAITING; IDLING;");

    br.newLine();

    DecimalFormatSymbols unusualSymbols = new DecimalFormatSymbols(Locale.GERMANY);
    unusualSymbols.setDecimalSeparator('.');
    String strange = "0.00";
    DecimalFormat myFormatter = new DecimalFormat(strange, unusualSymbols);

    for (int i = 0; i < timestamps; i++) {

      double sumNow = stoppingResult[i][0];

      double s1 = stoppingResult[i][1] > 0 ? stoppingResult[i][1] / sumNow : 0;
      double s2 = stoppingResult[i][2] > 0 ? stoppingResult[i][2] / sumNow : 0;
      double s3 = stoppingResult[i][3] > 0 ? stoppingResult[i][3] / sumNow : 0;
      double s4 = stoppingResult[i][4] > 0 ? stoppingResult[i][4] / sumNow : 0;

      String output1 = myFormatter.format(s1);
      String output2 = myFormatter.format(s2);
      String output3 = myFormatter.format(s3);
      String output4 = myFormatter.format(s4);

      try {
        double sum =
            Double.parseDouble(output1)
                + Double.parseDouble(output2)
                + Double.parseDouble(output3)
                + Double.parseDouble(output4);
        if (sum < 1.00) {
          s1 += 1.00 - sum;
          output1 = myFormatter.format(s1);
        }
      } catch (Exception e) {
        System.out.println(e);
      }

      br.write(output1 + "; " + output2 + "; " + output3 + "; " + output4);
      br.newLine();
    }
    br.close();
    System.out.println("statistic is written to system");
  }

  private synchronized void generateStoppingResult(long fac) {

    Double[][] result = new Double[timestamps][timeTypes.size() + 1];
    for (int i = 0; i < timestamps; i++) {
      for (int j = 0; j < timeTypes.size() + 1; j++) {
        result[i][j] = 0.0d;
      }
    }

    System.out.println(here() + " stoppingTime.size(): " + stoppingTime.size());
    //        long firstStartTime = stoppingTime.peekFirst().startTime;
    long firstStartTime = stoppingTime.get(0).startTime;

    for (Time t : stoppingTime) {
      t.startTime -= firstStartTime;
      t.endTime -= firstStartTime;
      long begin = 0;
      long end = begin + fac;

      for (int i = 0; i < timestamps; i++) {

        if (t.startTime >= begin && t.endTime <= end) {
          long l = t.endTime - t.startTime;
          result[i][0] += l;
          result[i][t.process] += l;
        } else if (t.startTime >= begin && t.startTime < end && t.endTime >= end) {
          long l = end - t.startTime;
          result[i][0] += l;
          result[i][t.process] += l;
        } else if (t.startTime <= begin && t.endTime > begin && t.endTime <= end) {
          long l = t.endTime - begin;
          result[i][0] += l;
          result[i][t.process] += l;
        } else if (t.startTime < begin && t.endTime > end) {
          long l = end - begin;
          result[i][0] += l;
          result[i][t.process] += l;
        }

        begin = end;
        end = begin + (long) fac;
      }
    }
    this.stoppingResult = result;
  }

  public synchronized void startLive() {
    long time = System.nanoTime();
    if (startTime == 0) {
      startTime = time;
    }

    if (lastStartStopLiveTimeStamp >= 0) {
      timeDead += time - lastStartStopLiveTimeStamp;
    }
    lastStartStopLiveTimeStamp = time;
  }

  public synchronized void stopLive() {
    long time = System.nanoTime();
    timeAlive += time - lastStartStopLiveTimeStamp;
    lastStartStopLiveTimeStamp = time;
  }

  public synchronized void collect(Logger logs[]) {
    for (Logger l : logs) {
      add(l);
    }
  }

  public synchronized void stats() {
    System.out.println(
        nodesGiven
            + " Task items stolen = "
            + nodesReceived
            + " (direct) + "
            + lifelineNodesReceived
            + " (lifeline).");
    System.out.println(stealsPerpetrated + " successful direct steals.");
    System.out.println(lifelineStealsPerpetrated + " successful lifeline steals.");
  }

  public synchronized void add(Logger other) {
    nodesCount += other.nodesCount;
    nodesGiven += other.nodesGiven;
    nodesReceived += other.nodesReceived;
    stealsPerpetrated += other.stealsPerpetrated;
    lifelineNodesReceived += other.lifelineNodesReceived;
    lifelineStealsPerpetrated += other.lifelineStealsPerpetrated;
    if (stoppingTime != null && other.stoppingTime != null) {
      System.err.println("Logger: StoppingTime != null");
      stoppingTime.addAll(other.stoppingTime);
    }

    for (int i = 0; i < timestamps; i++) {
      for (int j = 0; j < timeTypes.size() + 1; j++) {
        stoppingResult[i][j] += other.stoppingResult[i][j];
      }
    }
  }

  public synchronized Logger get() {
    return this;
  }

  public synchronized void stoppingTimeToResult() {
    if (stoppingTime == null) {
      if (here().id == 0) {
        System.out.println(
            "Logger:stoppingTimeToResult(): stoppingTime was not startet, wrong Constructor called?");
      }
      return;
    }

    long firstTime = stoppingTime.get(0).startTime;
    long lastTime = 0;
    for (Time t : stoppingTime) {
      if (t.endTime > lastTime) {
        lastTime = t.endTime;
      }
    }
    long fac = (lastTime - firstTime) / timestamps;
    this.generateStoppingResult(fac);
    this.stoppingTime = null;
  }

  @Override
  public String toString() {
    return this.placeID
        + " -> "
        + sub("" + (timeAlive / 1E9), 0, 6)
        + " : "
        + sub("" + (timeDead / 1E9), 0, 6)
        + " : "
        + sub("" + ((timeAlive + timeDead) / 1E9), 0, 6)
        + " : "
        + sub("" + (100.0 * timeAlive / (timeAlive + timeDead)), 0, 6)
        + "%"
        + " :: "
        + sub("" + ((startTime - timeReference) / 1E9), 0, 6)
        + " : "
        + sub("" + ((lastStartStopLiveTimeStamp - timeReference) / 1E9), 0, 6)
        + " :: "
        + nodesCount
        + " :: "
        + nodesGiven
        + " : "
        + nodesReceived
        + " : "
        + lifelineNodesReceived
        + " :: "
        + stealsReceived
        + " : "
        + lifelineStealsReceived
        + " :: "
        + stealsSuffered
        + " : "
        + lifelineStealsSuffered
        + " :: "
        + stealsAttempted
        + " : "
        + (stealsAttempted - stealsPerpetrated)
        + " :: "
        + lifelineStealsAttempted
        + " : "
        + (lifelineStealsAttempted - lifelineStealsPerpetrated)
        + " : "
        + timeReference;
  }

  private class Time implements Serializable {

    private static final long serialVersionUID = 1L;

    public long startTime;

    public long endTime;
    public int process;
    public boolean automaticStop;
    public int id;
    public int placeID;

    public Time(long now, int process, boolean automaticStop) {
      this(now, process, automaticStop, Logger.this.id);
      Logger.this.id++;
    }

    public Time(long now, int process, boolean automaticStop, int id) {
      this.process = process;
      this.startTime = now;
      this.endTime = 0;
      this.automaticStop = automaticStop;
      this.id = id;
      this.placeID = here().id;
    }
  }
}
