/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase2_x;

import com.google.cloud.bigtable.hbase.adapters.SampledRowKeysAdapter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AbstractBigtableConnection;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableBuilder;
import org.apache.hadoop.hbase.security.User;

/**
 * HBase 2.x specific implementation of {@link AbstractBigtableConnection}.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class BigtableConnection extends AbstractBigtableConnection {

  /**
   * Constructor for BigtableConnection.
   *
   * @param conf a {@link Configuration} object.
   * @throws IOException if any.
   */
  public BigtableConnection(Configuration conf) throws IOException {
    super(conf);
  }

  public BigtableConnection(Configuration conf, ExecutorService pool, User user)
      throws IOException {
    super(conf);
  }

  /**
   * Due to hbase 1.x to 2.x binary incompatibilities. {@link
   * HRegionLocation#HRegionLocation(org.apache.hadoop.hbase.client.RegionInfo, ServerName)} will
   * fail with NoSuchMethodException if not recompiled with hbase 2.0 dependencies. Hence the
   * override. See {@link SampledRowKeysAdapter} for more details.
   */
  @Override
  protected SampledRowKeysAdapter createSampledRowKeysAdapter(
      TableName tableName, ServerName serverName) {
    return new SampledRowKeysAdapter(tableName, serverName) {
      @Override
      protected HRegionLocation createRegionLocation(byte[] startKey, byte[] endKey) {
        RegionInfo regionInfo =
            RegionInfoBuilder.newBuilder(tableName).setStartKey(startKey).setEndKey(endKey).build();
        return new HRegionLocation(regionInfo, serverName);
      }
    };
  }

  /** {@inheritDoc} */
  @Override
  public Admin getAdmin() throws IOException {
    return new BigtableAdmin(this);
  }

  /** {@inheritDoc} */
  @Override
  public TableBuilder getTableBuilder(final TableName tableName, final ExecutorService pool) {
    return new TableBuilder() {

      @Override
      public TableBuilder setWriteRpcTimeout(int arg0) {
        return this;
      }

      @Override
      public TableBuilder setRpcTimeout(int arg0) {
        return this;
      }

      @Override
      public TableBuilder setReadRpcTimeout(int arg0) {
        return this;
      }

      @Override
      public TableBuilder setOperationTimeout(int arg0) {
        return this;
      }

      @Override
      public Table build() {
        try {
          return getTable(tableName, pool);
        } catch (IOException e) {
          throw new RuntimeException("Could not create the table", e);
        }
      }
    };
  }

  /** {@inheritDoc} */
  @Override
  public Table getTable(TableName tableName, ExecutorService ignored) throws IOException {
    return new BigtableTable(this, createAdapter(tableName));
  }

  @Override
  public void clearRegionLocationCache() {
    throw new UnsupportedOperationException("clearRegionLocationCache");
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.client.CommonConnection#getAllRegionInfos(org.apache.hadoop.hbase.TableName)
   */
  @Override
  public List<HRegionInfo> getAllRegionInfos(TableName tableName) throws IOException {
    List<HRegionInfo> regionInfos = new CopyOnWriteArrayList<>();
    for (HRegionLocation location : getRegionLocator(tableName).getAllRegionLocations()) {
      regionInfos.add(location.getRegionInfo());
    }
    return regionInfos;
  }
}
