package utils;

import static apgas.Constructs.here;
import static apgas.Constructs.place;

import apgas.impl.Config;
import apgas.impl.Worker;
import java.io.Serializable;

/** Created by jposner on 03.04.17. */
public class ConsolePrinter implements Serializable {

  private static final long startTime = System.nanoTime();

  private static final boolean PRINT =
      Boolean.parseBoolean(System.getProperty(Config.APGAS_CONSOLEPRINTER, "false"));
  private static final ConsolePrinter instance = new ConsolePrinter();

  public static synchronized ConsolePrinter getInstance() {
    return ConsolePrinter.instance;
  }

  public synchronized void println(String output) {
    if (PRINT == true) {
      String callerName = Thread.currentThread().getStackTrace()[2].getMethodName();
      int workerID = ((Worker) Thread.currentThread()).getMyID();
      long time = (long) ((System.nanoTime() - startTime) / 1E6);
      System.out.println(
          here() + "[" + workerID + "] (in " + callerName + ") [" + time + "]: " + output);
    }
  }

  public synchronized void printlnErr(String output) {
    if (PRINT == true) {
      String callerName = Thread.currentThread().getStackTrace()[2].getMethodName();
      long time = (long) ((System.nanoTime() - startTime) / 1E6);
      System.err.println(here() + " (in " + callerName + ") [" + time + "]: " + output);
    }
  }

  public synchronized void print(String output) {
    if (PRINT == true) {
      String callerName = Thread.currentThread().getStackTrace()[2].getMethodName();
      long time = (long) ((System.nanoTime() - startTime) / 1E6);
      System.out.print(here() + " (in " + callerName + ") [" + time + "]: " + output);
    }
  }

  public synchronized void printErr(String output) {
    if (PRINT == true) {
      String callerName = Thread.currentThread().getStackTrace()[2].getMethodName();
      long time = (long) ((System.nanoTime() - startTime) / 1E6);
      System.err.print(here() + " (in " + callerName + ") [" + time + "]: " + output);
    }
  }

  public synchronized void remotePrintln(int source, String output) {
    if (PRINT == true) {
      String callerName = Thread.currentThread().getStackTrace()[2].getMethodName();
      long time = (long) ((System.nanoTime() - startTime) / 1E6);
      System.out.println(
          place(source)
              + " (in "
              + callerName
              + " at "
              + here().id
              + ") ["
              + time
              + "]: "
              + output);
    }
  }

  public synchronized void remotePrintlnErr(int source, String output) {
    if (PRINT == true) {
      String callerName = Thread.currentThread().getStackTrace()[2].getMethodName();
      long time = (long) ((System.nanoTime() - startTime) / 1E6);
      System.err.println(
          place(source)
              + " (in "
              + callerName
              + " at "
              + here().id
              + ") ["
              + time
              + "]: "
              + output);
    }
  }

  public synchronized boolean getStatus() {
    return PRINT;
  }
}
