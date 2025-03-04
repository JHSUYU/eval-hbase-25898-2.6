/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.rest;

import java.io.IOException;
import java.util.Base64.Decoder;
import java.util.List;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.javax.ws.rs.DefaultValue;
import org.apache.hbase.thirdparty.javax.ws.rs.Encoded;
import org.apache.hbase.thirdparty.javax.ws.rs.HeaderParam;
import org.apache.hbase.thirdparty.javax.ws.rs.Path;
import org.apache.hbase.thirdparty.javax.ws.rs.PathParam;
import org.apache.hbase.thirdparty.javax.ws.rs.QueryParam;

@InterfaceAudience.Private
public class TableResource extends ResourceBase {

  String table;
  private static final Logger LOG = LoggerFactory.getLogger(TableResource.class);

  private static final Decoder base64Urldecoder = java.util.Base64.getUrlDecoder();

  /**
   * Constructor
   */
  public TableResource(String table) throws IOException {
    super();
    this.table = table;
  }

  /** Returns the table name */
  String getName() {
    return table;
  }

  /** Returns true if the table exists n */
  boolean exists() throws IOException {
    return servlet.getAdmin().tableExists(TableName.valueOf(table));
  }

  @Path("exists")
  public ExistsResource getExistsResource() throws IOException {
    return new ExistsResource(this);
  }

  @Path("regions")
  public RegionsResource getRegionsResource() throws IOException {
    return new RegionsResource(this);
  }

  @Path("scanner")
  public ScannerResource getScannerResource() throws IOException {
    return new ScannerResource(this);
  }

  @Path("schema")
  public SchemaResource getSchemaResource() throws IOException {
    return new SchemaResource(this);
  }

  @Path("{multiget: multiget.*}")
  public MultiRowResource getMultipleRowResource(final @QueryParam("v") String versions,
    @PathParam("multiget") String path) throws IOException {
    return new MultiRowResource(this, versions, path.replace("multiget", "").replace("/", ""));
  }

  @Path("{rowspec: [^*]+}")
  public RowResource getRowResource(
    // We need the @Encoded decorator so Jersey won't urldecode before
    // the RowSpec constructor has a chance to parse
    final @PathParam("rowspec") @Encoded String rowspec, final @QueryParam("v") String versions,
    final @QueryParam("check") String check, final @QueryParam("rr") String returnResult,
    final @HeaderParam("Encoding") String keyEncodingHeader,
    final @QueryParam(Constants.KEY_ENCODING_QUERY_PARAM_NAME) String keyEncodingQuery)
    throws IOException {
    String keyEncoding = (keyEncodingHeader != null) ? keyEncodingHeader : keyEncodingQuery;
    return new RowResource(this, rowspec, versions, check, returnResult, keyEncoding);
  }

  // TODO document
  @Path("{suffixglobbingspec: .*\\*/.+}")
  public RowResource getRowResourceWithSuffixGlobbing(
    // We need the @Encoded decorator so Jersey won't urldecode before
    // the RowSpec constructor has a chance to parse
    final @PathParam("suffixglobbingspec") @Encoded String suffixglobbingspec,
    final @QueryParam("v") String versions, final @QueryParam("check") String check,
    final @QueryParam("rr") String returnResult,
    final @HeaderParam("Encoding") String keyEncodingHeader,
    final @QueryParam(Constants.KEY_ENCODING_QUERY_PARAM_NAME) String keyEncodingQuery)
    throws IOException {
    String keyEncoding = (keyEncodingHeader != null) ? keyEncodingHeader : keyEncodingQuery;
    return new RowResource(this, suffixglobbingspec, versions, check, returnResult, keyEncoding);
  }

  // TODO document
  // FIXME handle binary rowkeys (like put and delete does)
  @Path("{scanspec: .*[*]$}")
  public TableScanResource getScanResource(final @PathParam("scanspec") String scanSpec,
    @DefaultValue(Integer.MAX_VALUE + "") @QueryParam(Constants.SCAN_LIMIT) int userRequestedLimit,
    @DefaultValue("") @QueryParam(Constants.SCAN_START_ROW) String startRow,
    @DefaultValue("") @QueryParam(Constants.SCAN_END_ROW) String endRow,
    @QueryParam(Constants.SCAN_COLUMN) List<String> column,
    @DefaultValue("1") @QueryParam(Constants.SCAN_MAX_VERSIONS) int maxVersions,
    @DefaultValue("-1") @QueryParam(Constants.SCAN_BATCH_SIZE) int batchSize,
    @DefaultValue("0") @QueryParam(Constants.SCAN_START_TIME) long startTime,
    @DefaultValue(Long.MAX_VALUE + "") @QueryParam(Constants.SCAN_END_TIME) long endTime,
    @DefaultValue("true") @QueryParam(Constants.SCAN_CACHE_BLOCKS) boolean cacheBlocks,
    @DefaultValue("false") @QueryParam(Constants.SCAN_REVERSED) boolean reversed,
    @QueryParam(Constants.FILTER) String paramFilter,
    @QueryParam(Constants.FILTER_B64) @Encoded String paramFilterB64,
    @DefaultValue("true") @QueryParam(Constants.SCAN_INCLUDE_START_ROW) boolean includeStartRow,
    @DefaultValue("false") @QueryParam(Constants.SCAN_INCLUDE_STOP_ROW) boolean includeStopRow) {
    try {
      Filter prefixFilter = null;
      Scan tableScan = new Scan();
      if (scanSpec.indexOf('*') > 0) {
        String prefix = scanSpec.substring(0, scanSpec.indexOf('*'));
        byte[] prefixBytes = Bytes.toBytes(prefix);
        prefixFilter = new PrefixFilter(Bytes.toBytes(prefix));
        if (startRow.isEmpty()) {
          tableScan.withStartRow(prefixBytes, includeStartRow);
        }
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace("Query parameters  : Table Name = > " + this.table + " Start Row => " + startRow
          + " End Row => " + endRow + " Columns => " + column + " Start Time => " + startTime
          + " End Time => " + endTime + " Cache Blocks => " + cacheBlocks + " Max Versions => "
          + maxVersions + " Batch Size => " + batchSize);
      }
      Table hTable = RESTServlet.getInstance().getTable(this.table);
      tableScan.setBatch(batchSize);
      tableScan.setMaxVersions(maxVersions);
      tableScan.setTimeRange(startTime, endTime);
      if (!startRow.isEmpty()) {
        tableScan.withStartRow(Bytes.toBytes(startRow), includeStartRow);
      }
      tableScan.withStopRow(Bytes.toBytes(endRow), includeStopRow);
      for (String col : column) {
        byte[][] parts = CellUtil.parseColumn(Bytes.toBytes(col.trim()));
        if (parts.length == 1) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Scan family : " + Bytes.toStringBinary(parts[0]));
          }
          tableScan.addFamily(parts[0]);
        } else if (parts.length == 2) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Scan family and column : " + Bytes.toStringBinary(parts[0]) + "  "
              + Bytes.toStringBinary(parts[1]));
          }
          tableScan.addColumn(parts[0], parts[1]);
        } else {
          throw new IllegalArgumentException("Invalid column specifier.");
        }
      }
      FilterList filterList = new FilterList();
      byte[] filterBytes = null;
      if (paramFilterB64 != null) {
        filterBytes = base64Urldecoder.decode(paramFilterB64);
      } else if (paramFilter != null) {
        filterBytes = paramFilter.getBytes();
      }
      if (filterBytes != null) {
        // Note that this is a completely different representation of the filters
        // than the JSON one used in the /table/scanner endpoint
        ParseFilter pf = new ParseFilter();
        Filter parsedParamFilter = pf.parseFilterString(filterBytes);
        if (parsedParamFilter != null) {
          filterList.addFilter(parsedParamFilter);
        }
      }
      if (prefixFilter != null) {
        filterList.addFilter(prefixFilter);
      }
      if (filterList.size() > 0) {
        tableScan.setFilter(filterList);
      }

      int fetchSize = this.servlet.getConfiguration().getInt(Constants.SCAN_FETCH_SIZE, 10);
      tableScan.setCaching(fetchSize);
      tableScan.setReversed(reversed);
      tableScan.setCacheBlocks(cacheBlocks);
      return new TableScanResource(hTable.getScanner(tableScan), userRequestedLimit);
    } catch (IOException exp) {
      servlet.getMetrics().incrementFailedScanRequests(1);
      processException(exp);
      LOG.warn(exp.toString(), exp);
      return null;
    }
  }
}
