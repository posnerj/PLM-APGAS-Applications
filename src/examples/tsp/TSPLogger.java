package examples.tsp;

import java.io.Serializable;

/** Created by jonas on 22.08.17. */
public class TSPLogger implements Serializable {

  private final long[] calls;
  private final long[] cuts;
  private final long[] asyncAny;
  private final long[] recursive;
  private final long[] foundResultsLocal;
  private final long[] foundResultsGlobal;
  private final long[] processedNodes;

  public TSPLogger(int count) {
    this.calls = new long[count];
    this.cuts = new long[count];
    this.asyncAny = new long[count];
    this.recursive = new long[count];
    this.foundResultsLocal = new long[count];
    this.foundResultsGlobal = new long[count];
    this.processedNodes = new long[count];
  }

  public void add(TSPLogger other) {
    for (int i = 0; i < calls.length; i++) {
      this.calls[i] += other.calls[i];
      this.cuts[i] += other.cuts[i];
      this.asyncAny[i] += other.asyncAny[i];
      this.recursive[i] += other.recursive[i];
      this.foundResultsLocal[i] += other.foundResultsLocal[i];
      this.foundResultsGlobal[i] += other.foundResultsGlobal[i];
      this.processedNodes[i] += other.processedNodes[i];
    }
  }

  public String toString() {
    if (this.calls.length == 0) {
      return "Logger was disabled";
    }
    long sumCalls = 0;
    long sumCuts = 0;
    long sumAsyncAny = 0;
    long sumRecursive = 0;
    long sumFoundResultsLocal = 0;
    long sumFoundResultsGlobal = 0;
    long sumProcessedNodes = 0;

    for (int i = 0; i < calls.length; i++) {
      sumCalls += this.calls[i];
      sumCuts += this.cuts[i];
      sumAsyncAny += this.asyncAny[i];
      sumRecursive += this.recursive[i];
      sumFoundResultsLocal += this.foundResultsLocal[i];
      sumFoundResultsGlobal += this.foundResultsGlobal[i];
      sumProcessedNodes += this.processedNodes[i];
    }

    StringBuilder result = new StringBuilder();
    result.append("Calls: " + sumCalls);
    result.append(System.getProperty("line.separator"));
    result.append("Cuts: " + sumCuts);
    result.append(System.getProperty("line.separator"));
    result.append("AsyncAnyCalls: " + sumAsyncAny);
    result.append(System.getProperty("line.separator"));
    result.append("RecursiveCalls: " + sumRecursive);
    result.append(System.getProperty("line.separator"));
    result.append("foundResultsLocal: " + sumFoundResultsLocal);
    result.append(System.getProperty("line.separator"));
    result.append("foundResultsGlobal: " + sumFoundResultsGlobal);
    result.append(System.getProperty("line.separator"));
    result.append("processedNodes: " + sumProcessedNodes);
    return result.toString();
  }

  public void incrementCalls(int pos) {
    if (this.calls.length == 0) {
      return;
    }
    this.calls[pos]++;
  }

  public void incrementCuts(int pos) {
    if (this.cuts.length == 0) {
      return;
    }
    this.cuts[pos]++;
  }

  public void incrementAsyncAny(int pos) {
    if (this.asyncAny.length == 0) {
      return;
    }
    this.asyncAny[pos]++;
  }

  public void incrementRecursive(int pos) {
    if (this.recursive.length == 0) {
      return;
    }
    this.recursive[pos]++;
  }

  public void incrementFoundResultsLocal(int pos) {
    if (this.foundResultsLocal.length == 0) {
      return;
    }
    this.foundResultsLocal[pos]++;
  }

  public void incrementFoundResultsGlobal(int pos) {
    if (this.foundResultsGlobal.length == 0) {
      return;
    }
    this.foundResultsGlobal[pos]++;
  }

  public void addToProcessedNodes(int pos, int value) {
    if (this.foundResultsGlobal.length == 0) {
      return;
    }
    this.processedNodes[pos] += value;
  }
}
