/*
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.hbase;

import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;
import java.util.concurrent.TimeUnit;
import org.junit.ClassRule;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
  TestAppend.class,
  TestAuth.class,
  TestBasicOps.class,
  TestBatch.class,
  TestBufferedMutator.class,
  TestCheckAndMutate.class,
  TestColumnFamilyAdmin.class,
  TestCreateTable.class,
  TestDisableTable.class,
  TestDelete.class,
  TestDurability.class,
  TestFilters.class,
  TestSingleColumnValueFilter.class,
  TestGet.class,
  TestGetTable.class,
  TestScan.class,
  TestSnapshots.class,
  TestIncrement.class,
  TestListTables.class,
  TestPut.class,
  TestTimestamp.class,
  TestTruncateTable.class,
  TestModifyTable.class
})
public class IntegrationTests {
  private static final int TIME_OUT_MINUTES =
      Integer.getInteger("integration.test.timeout.minutes", 3);

  @ClassRule public static Timeout timeoutRule = new Timeout(TIME_OUT_MINUTES, TimeUnit.MINUTES);

  @ClassRule public static SharedTestEnvRule sharedTestEnvRule = SharedTestEnvRule.getInstance();
}
