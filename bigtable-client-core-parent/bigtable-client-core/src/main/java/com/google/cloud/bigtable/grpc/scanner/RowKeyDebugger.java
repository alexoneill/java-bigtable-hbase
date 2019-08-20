package com.google.cloud.bigtable.grpc.scanner;

import com.google.bigtable.v2.ReadRowsResponse.CellChunk;
import com.google.cloud.bigtable.grpc.scanner.RowMerger.RowInProgress;
import com.google.cloud.bigtable.grpc.scanner.RowMerger.RowMergerState;
import com.google.cloud.bigtable.util.ByteStringComparator;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

public class RowKeyDebugger {
  private final int NUM_ENTRIES = 10;

  private static final Logger LOGGER = Logger.getLogger(RowKeyDebugger.class.getName());
  private static AtomicBoolean firstTimeInit = new AtomicBoolean();

  private final AtomicBoolean committing = new AtomicBoolean();
  private AtomicReference<DebugKey> beforeCommitKey = new AtomicReference<>();
  private final ArrayDeque<DebugKey> debugLastKeys = new ArrayDeque<>(NUM_ENTRIES);

  static RowKeyDebugger create() {
    if (firstTimeInit.compareAndSet(false, true)) {
      runFirstTimeInit();
    }
    return new RowKeyDebugger();
  }

  public static void runFirstTimeInit() {
    LOGGER.info("info-Initializing cloud bigtable client with row key debugging");
    LOGGER.info("error-Initializing cloud bigtable client with row key debugging");

    // Verify that the fix from 1.12.0 is in place
    // Must use reflection since the fix was implemented in bigtable-hbase layer.
    try {
      ByteString key = ByteString.copyFromUtf8("test-key");
      FlatRow row =
          FlatRow.newBuilder()
              .withRowKey(key)
              .addCell("cf", ByteString.copyFromUtf8("q"), 1000, ByteString.copyFromUtf8("value"))
              .build();

      Class<?> rowAdapterCls =
          Class.forName("com.google.cloud.bigtable.hbase.adapters.read.FlatRowAdapter");
      Object rowAdapter = rowAdapterCls.newInstance();
      Method adaptMethod = rowAdapterCls.getMethod("adaptResponse", FlatRow.class);
      Object result = adaptMethod.invoke(rowAdapter, row);
      Class<?> resultClass = Class.forName("org.apache.hadoop.hbase.client.Result");
      Method getRowMethod = resultClass.getMethod("getRow");
      byte[] rowKeyBytes = (byte[]) getRowMethod.invoke(result);

      rowKeyBytes[0] = 'x';

      if (!"test-key".equals(key.toStringUtf8())) {
        LOGGER.severe("RowAdapter fix is missing!");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  void onNextAfterComplete() {
    logError("RowMerger got response after it was logically complete");
  }

  public void beforeValidateChunk(
      ByteString actualLastKey, RowMergerState state, RowInProgress rowInProgress) {
    DebugKey lastKey = getLastKey();

    ensureLastKeyMatchesExpected("beforeValidateChunk state=" + state, lastKey, actualLastKey);
  }

  void onResetRow() {
    ensureNotCommitting();
    logError("Got reset row");
  }

  void beforeNewRowUpdateKey(ByteString actualLastKey, ByteString rowKey) {
    DebugKey lastKey = getLastKey();

    ensureNotCommitting();

    // Make sure that the last key is in sync
    if (!ensureLastKeyMatchesExpected("beforeNewRowUpdateKey", lastKey, actualLastKey)) {
      return;
    }

    // Make sure that the new row row key is in correct order
    if (!ensureStrictlyIncreasing("beforeNewRowUpdateKey", lastKey, rowKey)) {
      return;
    }
  }

  void beforeUpdateRowInProgressKey(ByteString actualLastKey, CellChunk chunk) {
    DebugKey lastKey = getLastKey();

    ensureNotCommitting();
    ensureLastKeyMatchesExpected("beforeUpdateRowInProgressKey", lastKey, actualLastKey);
  }

  void beforeUpdateChunk(ByteString actualLastKey, CellChunk chunk) {
    DebugKey lastKey = getLastKey();

    ensureNotCommitting();
    ensureLastKeyMatchesExpected("beforeUpdateChunk", lastKey, actualLastKey);
  }

  void beforeCommit(CellChunk chunk, ByteString actualLastKey, RowInProgress rowInProgress) {
    DebugKey lastKey = getLastKey();

    ensureNotCommitting();

    committing.set(true);
    beforeCommitKey.set(new DebugKey(rowInProgress.getRowKey()));

    if (!ensureLastKeyMatchesExpected("beforeCommit", lastKey, actualLastKey)) {
      return;
    }
    if (!ensureStrictlyIncreasing("beforeCommit", lastKey, rowInProgress.getRowKey())) {
      return;
    }
  }

  void afterCommit(CellChunk chunk, ByteString newLastKey, RowInProgress rowInProgress) {
    DebugKey lastKey = getLastKey();

    if (!committing.compareAndSet(true, false)) {
      logError("Commit flag not set for afterCommit");
    }

    // Make sure that the new key didn't mutate in between the commit
    ensureLastKeyMatchesExpected("afterCommit", beforeCommitKey.get(), newLastKey);

    // Make sure that the new key is in order
    ensureStrictlyIncreasing("afterCommit", lastKey, newLastKey);

    // Update debug state
    addNewRowKey(newLastKey);
    beforeCommitKey.set(null);
  }

  private boolean ensureNotCommitting() {
    if (committing.get()) {
      logError("Commit flag unexpectedly set!");
      return false;
    }
    return true;
  }

  private boolean ensureLastKeyMatchesExpected(
      String msg, DebugKey expectedLastKey, ByteString actualLastKey) {
    if (expectedLastKey == null) {
      if (actualLastKey != null) {
        logError(msg + ": Expected last key is null, but actual is not!");
        return false;
      }
      return true;
    }

    boolean ok = true;

    // Referential equality
    if (expectedLastKey.original != actualLastKey) {
      logError(msg + ": Last key is a different object then expected");
      ok = false;
    }

    if (!Objects.equals(expectedLastKey.copy, actualLastKey)) {
      String actualLastKeyStr = "{{null}}";
      if (actualLastKey != null) {
        actualLastKeyStr = actualLastKey.toStringUtf8();
      }
      logError(msg + ": Unexpected actual last key: " + actualLastKeyStr);

      ok = false;
    }

    return ok;
  }

  private boolean ensureStrictlyIncreasing(String msg, DebugKey lastKey, ByteString newKey) {
    if (newKey == null) {
      logError(msg + ": newKey is null!");
      return false;
    }

    if (lastKey != null) {
      if (ByteStringComparator.INSTANCE.compare(lastKey.copy, newKey) >= 0) {
        logError(
            msg + ": New key should come after lastKey. newRowKey: " + lastKey.copy.toStringUtf8());

        return false;
      }
    }
    return true;
  }

  private void logError(String s) {
    String msg = String.format("[%s] %s", Thread.currentThread().getId(), s);
    LOGGER.severe(msg);

    LOGGER.severe("last " + NUM_ENTRIES + " keys");
    for (DebugKey key : getAllKeys()) {
      if (Arrays.equals(key.original.toByteArray(), key.copy.toByteArray())) {
        LOGGER.severe(key.copy.toStringUtf8());
      } else {
        LOGGER.severe(
            String.format("%s != %s", key.copy.toStringUtf8(), key.original.toStringUtf8()));
      }
    }
  }

  private synchronized void addNewRowKey(ByteString rowKey) {
    if (debugLastKeys.size() >= NUM_ENTRIES) {
      debugLastKeys.removeFirst();
    }

    debugLastKeys.add(new DebugKey(rowKey));
  }

  private synchronized DebugKey getLastKey() {
    return debugLastKeys.peekLast();
  }

  private synchronized ImmutableList<DebugKey> getAllKeys() {
    return ImmutableList.copyOf(debugLastKeys);
  }

  public void onClearRowInProgress() {
    logError("onClearRowInProgress");
    beforeCommitKey.set(null);
    committing.set(false);
  }

  private static final class DebugKey {
    private final ByteString original;
    private final ByteString copy;

    public DebugKey(ByteString original) {
      this.original = original;
      ByteString myCopy;
      try {
        myCopy = ByteString.readFrom(original.newInput());
      } catch (IOException e) {
        e.printStackTrace();
        myCopy = original;
      }
      this.copy = myCopy;
    }
  }
}
