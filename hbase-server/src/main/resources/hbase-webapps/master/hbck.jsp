<%--
/**
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
--%>
<%@ page contentType="text/html;charset=UTF-8"
         import="java.time.Instant"
         import="java.time.ZoneId"
         import="java.util.List"
         import="java.util.Map"
         import="java.util.stream.Collectors"
         import="java.time.ZonedDateTime"
         import="java.time.format.DateTimeFormatter"
%>
<%@ page import="org.apache.hadoop.fs.Path" %>
<%@ page import="org.apache.hadoop.hbase.client.RegionInfo" %>
<%@ page import="org.apache.hadoop.hbase.master.hbck.HbckChore" %>
<%@ page import="org.apache.hadoop.hbase.master.hbck.HbckReport" %>
<%@ page import="org.apache.hadoop.hbase.master.HMaster" %>
<%@ page import="org.apache.hadoop.hbase.master.ServerManager" %>
<%@ page import="org.apache.hadoop.hbase.ServerName" %>
<%@ page import="org.apache.hadoop.hbase.util.Bytes" %>
<%@ page import="org.apache.hadoop.hbase.util.Pair" %>
<%@ page import="org.apache.hadoop.hbase.master.janitor.CatalogJanitor" %>
<%@ page import="org.apache.hadoop.hbase.master.janitor.CatalogJanitorReport" %>
<%@ page import="java.util.Optional" %>
<%@ page import="org.apache.hadoop.hbase.util.EnvironmentEdgeManager" %>
<%
  final String cacheParameterValue = request.getParameter("cache");
  final HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
  pageContext.setAttribute("pageTitle", "HBase Master HBCK Report: " + master.getServerName());
  if (!Boolean.parseBoolean(cacheParameterValue)) {
    // Run the two reporters inline w/ drawing of the page. If exception, will show in page draw.
    try {
      master.getMasterRpcServices().runHbckChore(null, null);
    } catch (org.apache.hbase.thirdparty.com.google.protobuf.ServiceException se) {
      out.write("Failed generating a new hbck_chore report; using cache; try again or run hbck_chore_run in the shell: " + se.getMessage() + "\n");
    } 
    try {
      master.getMasterRpcServices().runCatalogScan(null, null);
    } catch (org.apache.hbase.thirdparty.com.google.protobuf.ServiceException se) {
      out.write("Failed generating a new catalogjanitor report; using cache; try again or run catalogjanitor_run in the shell: " + se.getMessage() + "\n");
    } 
  }
  HbckChore hbckChore = master.getHbckChore();
  HbckReport hbckReport = hbckChore == null ? null : hbckChore.getLastReport();
  String hbckReportStartTime = Optional.ofNullable(hbckReport)
    .map(HbckReport::getCheckingStartTimestamp)
    .map(start -> ZonedDateTime.ofInstant(start, ZoneId.systemDefault()))
    .map(zdt -> zdt.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))
    .orElse(null);
  String hbckReportEndTime = Optional.ofNullable(hbckReport)
    .map(HbckReport::getCheckingEndTimestamp)
    .map(start -> ZonedDateTime.ofInstant(start, ZoneId.systemDefault()))
    .map(zdt -> zdt.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))
    .orElse(null);
  CatalogJanitor cj = master.getCatalogJanitor();
  CatalogJanitorReport cjReport = cj == null? null: cj.getLastReport();
  final ServerManager serverManager = master.getServerManager();
%>
<jsp:include page="header.jsp">
  <jsp:param name="pageTitle" value="${pageTitle}"/>
</jsp:include>

<div class="container-fluid content">
  <% if (!master.isInitialized()) { %>
  <div class="row">
    <div class="page-header">
      <h1>Master is not initialized</h1>
    </div>
  </div>
  <jsp:include page="redirect.jsp" />
  <% } else { %>

  <div class="row">
    <div class="page-header">
      <p><span>This page displays two reports: the <em>HBCK Chore Report</em> and
        the <em>CatalogJanitor Consistency Issues</em> report. Only report titles
        show if there are no problems to list. Note some conditions are
        <strong>transitory</strong> as regions migrate. Reports are generated
        when you invoke this page unless you add <em>?cache=true</em> to the URL. Then
        we display the reports cached from the last time the reports were run.
        Reports are run by Chores that are hosted by the Master on a cadence.
        You can also run them on demand from the hbase shell: invoke <em>catalogjanitor_run</em>
        and/or <em>hbck_chore_run</em>. 
        ServerNames will be links if server is live, italic if dead, and plain if unknown.</span></p>
    </div>
  </div>
  <div class="row">
    <div class="page-header">
      <h1>HBCK Chore Report</h1>
      <p>
        <% if (hbckChore == null) { %>
          <span>HBCK chore has not yet initialized. Try again later.</span>
        <% } else if (hbckChore.isDisabled()) { %>
          <span>HBCK chore is currently disabled. Set hbase.master.hbck.chore.interval > 0 in the config & do a rolling-restart to enable it.</span>
        <% } else if (hbckReport == null) { %>
          <span>No Report created.</span>
        <% } else if (hbckReportStartTime != null && hbckReportEndTime == null) { %>
          <span>Checking started at <%= hbckReportStartTime %>. Please wait for checking to generate a new sub-report.</span>
        <% } else { %>
          <span>Checking started at <%= hbckReportStartTime %> and generated catalogJanitorReport at <%= hbckReportEndTime %>.</span>
        <% } %>
      </p>
    </div>
  </div>

  <% if (hbckReport != null && hbckReport.getInconsistentRegions().size() > 0) { %>
  <div class="row">
    <div class="page-header">
      <h2>Inconsistent Regions</h2>
    </div>
  </div>
      <p>
        <span>
        There are three cases: 1. Master thought this region opened, but no regionserver reported it (Fix: use assign
        command); 2. Master thought this region opened on Server1, but regionserver reported Server2 (Fix:
        need to check the server still exists. If not, schedule <em>ServerCrashProcedure</em> for it. If exists,
        restart Server2 and Server1):
        3. More than one regionserver reports opened this region (Fix: restart the RegionServers).
        Note: the reported online regionservers may be not be up-to-date when there are regions in transition.
        </span>
      </p>

  <table class="table table-striped">
    <tr>
      <th>Region Name</th>
      <th>Location in META</th>
      <th>Reported Online RegionServers</th>
    </tr>
    <% for (Map.Entry<String, Pair<ServerName, List<ServerName>>> entry : hbckReport.getInconsistentRegions().entrySet()) { %>
    <tr>
      <td><%= entry.getKey() %></td>
      <td><%= formatServerName(master, serverManager, entry.getValue().getFirst()) %></td>
      <td><%= entry.getValue().getSecond().stream().map(s -> formatServerName(master, serverManager, s)).
        collect(Collectors.joining(", ")) %></td>
    </tr>
    <% } %>
    <p><%= hbckReport.getInconsistentRegions().size() %> region(s) in set.</p>
  </table>
  <% } %>

  <% if (hbckReport != null && hbckReport.getOrphanRegionsOnRS().size() > 0) { %>
  <div class="row">
    <div class="page-header">
      <h2>Orphan Regions on RegionServer</h2>
    </div>
  </div>

  <table class="table table-striped">
    <tr>
      <th>Region Name</th>
      <th>Reported Online RegionServer</th>
    </tr>
    <% for (Map.Entry<String, ServerName> entry : hbckReport.getOrphanRegionsOnRS().entrySet()) { %>
    <tr>
      <td><%= entry.getKey() %></td>
      <td><%= formatServerName(master, serverManager, entry.getValue()) %></td>
    </tr>
    <% } %>
    <p><%= hbckReport.getOrphanRegionsOnRS().size() %> region(s) in set.</p>
  </table>
  <% } %>

  <% if (hbckReport != null && hbckReport.getOrphanRegionsOnFS().size() > 0) { %>
  <div class="row">
    <div class="page-header">
      <h2>Orphan Regions on FileSystem</h2>
    </div>
  </div>
      <p>
        <span>
          The below are Regions we've lost account of. To be safe, run bulk load of any data found under these Region orphan directories to have the
          cluster re-adopt data.
          First make sure <em>hbase:meta</em> is in a healthy state, that there are no holes, overlaps or inconsistencies (else bulk load may fail);
          run <em>hbck2 fixMeta</em>. Once this is done, per Region below, run a bulk
          load -- <em>$ hbase completebulkload REGION_DIR_PATH TABLE_NAME</em> -- and then delete the desiccated directory content (HFiles are removed upon
          successful load; all that is left are empty directories and occasionally a seqid marking file).
        </span>
      </p>
  <table class="table table-striped">
    <tr>
      <th>Region Encoded Name</th>
      <th>FileSystem Path</th>
    </tr>
    <% for (Map.Entry<String, Path> entry : hbckReport.getOrphanRegionsOnFS().entrySet()) { %>
    <tr>
      <td><%= entry.getKey() %></td>
      <td><%= entry.getValue() %></td>
    </tr>
    <% } %>

    <p><%= hbckReport.getOrphanRegionsOnFS().size() %> region(s) in set.</p>
  </table>
  <% } %>

  <%
    Instant nowInstant = Instant.ofEpochMilli(EnvironmentEdgeManager.currentTime());
    ZonedDateTime nowZdt = ZonedDateTime.ofInstant(nowInstant, ZoneId.systemDefault());
    String iso8601Now = nowZdt.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    String cjReportTime = Optional.ofNullable(cjReport)
      .map(CatalogJanitorReport::getCreateTime)
      .map(Instant::ofEpochMilli)
      .map(start -> ZonedDateTime.ofInstant(start, ZoneId.systemDefault()))
      .map(zdt -> zdt.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))
      .orElse(null);
  %>
  <div class="row inner_header">
    <div class="page-header">
      <h1>CatalogJanitor <em>hbase:meta</em> Consistency Issues</h1>
      <p>
        <% if (cjReport != null) { %>
          <span>Report created: <%= cjReportTime %> (now=<%= iso8601Now %>).</span></p>
        <% } else { %>
          <span>No catalogJanitorReport created.</span>
        <% } %>
    </div>
  </div>
  <% if (cjReport != null && !cjReport.isEmpty()) { %>
      <% if (!cjReport.getHoles().isEmpty()) { %>
          <div class="row inner_header">
            <div class="page-header">
              <h2>Holes</h2>
            </div>
          </div>
          <table class="table table-striped">
            <tr>
              <th>RegionInfo</th>
              <th>RegionInfo</th>
            </tr>
            <% for (Pair<RegionInfo, RegionInfo> p : cjReport.getHoles()) { %>
            <tr>
              <td><span title="<%= p.getFirst() %>"><%= p.getFirst().getRegionNameAsString() %></span></td>
              <td><span title="<%= p.getSecond() %>"><%= p.getSecond().getRegionNameAsString() %></span></td>
            </tr>
            <% } %>

            <p><%= cjReport.getHoles().size() %> hole(s).</p>
          </table>
      <% } %>
      <% if (!cjReport.getOverlaps().isEmpty()) { %>
            <div class="row inner_header">
              <div class="page-header">
                <h2>Overlaps</h2>
                <p>
                  <span>
                    Regions highlighted in <font color="blue">blue</font> are recently merged regions, HBase is still doing cleanup for them. Overlaps involving these regions cannot be fixed by <em>hbck2 fixMeta</em> at this moment.
                    Please wait some time, run <i>catalogjanitor_run</i> in hbase shell, refresh ‘HBCK Report’ page, make sure these regions are not highlighted to start the fix.
                  </span>
                </p>
              </div>
            </div>
            <table class="table table-striped">
              <tr>
                <th>RegionInfo</th>
                <th>Other RegionInfo</th>
              </tr>
              <% for (Pair<RegionInfo, RegionInfo> p : cjReport.getOverlaps()) { %>
              <tr>
                <% if (cjReport.getMergedRegions().containsKey(p.getFirst())) { %>
                  <td><span style="color:blue;" title="<%= p.getFirst() %>"><%= p.getFirst().getRegionNameAsString() %></span></td>
                <% } else { %>
                  <td><span title="<%= p.getFirst() %>"><%= p.getFirst().getRegionNameAsString() %></span></td>
                <% } %>
                <% if (cjReport.getMergedRegions().containsKey(p.getSecond())) { %>
                  <td><span style="color:blue;" title="<%= p.getSecond() %>"><%= p.getSecond().getRegionNameAsString() %></span></td>
                <% } else { %>
                  <td><span title="<%= p.getSecond() %>"><%= p.getSecond().getRegionNameAsString() %></span></td>
                <% } %>
              </tr>
              <% } %>

              <p><%= cjReport.getOverlaps().size() %> overlap(s).</p>
            </table>
      <% } %>
      <% if (!cjReport.getUnknownServers().isEmpty()) { %>
            <div class="row inner_header">
              <div class="page-header">
                <h2>Unknown Servers</h2>
              </div>
            </div>
            <p>
              <span>The below are servers mentioned in the hbase:meta table that are no longer 'live' or known 'dead'.
                The server likely belongs to an older cluster epoch since replaced by a new instance because of a restart/crash.
                To clear 'Unknown Servers', run 'hbck2 scheduleRecoveries UNKNOWN_SERVERNAME'. This will schedule a ServerCrashProcedure.
                It will clear out 'Unknown Server' references and schedule reassigns of any Regions that were associated with this host.
                But first!, be sure the referenced Region is not currently stuck looping trying to OPEN. Does it show as a Region-In-Transition on the
                Master home page? Is it mentioned in the 'Procedures and Locks' Procedures list? If so, perhaps it stuck in a loop
                trying to OPEN but unable to because of a missing reference or file.
                Read the Master log looking for the most recent
                mentions of the associated Region name. Try and address any such complaint first. If successful, a side-effect
                should be the clean up of the 'Unknown Servers' list. It may take a while. OPENs are retried forever but the interval
                between retries grows. The 'Unknown Server' may be cleared because it is just the last RegionServer the Region was
                successfully opened on; on the next open, the 'Unknown Server' will be purged.
              </span>
            </p>
            <table class="table table-striped">
              <tr>
                <th>RegionInfo</th>
                <th>ServerName</th>
              </tr>
              <% for (Pair<RegionInfo, ServerName> p: cjReport.getUnknownServers()) { %>
              <tr>
                <td><span title="<%= p.getFirst() %>"><%= p.getFirst().getRegionNameAsString() %></span></td>
                <td><%= p.getSecond() %></td>
              </tr>
              <% } %>

              <p><%= cjReport.getUnknownServers().size() %> unknown servers(s).</p>
            </table>
      <% } %>
      <% if (!cjReport.getEmptyRegionInfo().isEmpty()) { %>
            <div class="row inner_header">
              <div class="page-header">
                <h2>Empty <em>info:regioninfo</em></h2>
              </div>
            </div>
            <table class="table table-striped">
              <tr>
                <th>Row</th>
              </tr>
              <% for (byte [] row: cjReport.getEmptyRegionInfo()) { %>
              <tr>
                <td><%= Bytes.toStringBinary(row) %></td>
              </tr>
              <% } %>

              <p><%= cjReport.getEmptyRegionInfo().size() %> emptyRegionInfo(s).</p>
            </table>
      <% } %>
  <% } %>

  <% } %>
</div>

<jsp:include page="footer.jsp"/>

<%!
/**
 * Format serverName for display.
 * If a live server reference, make it a link.
 * If dead, make it italic.
 * If unknown, make it plain.
 * If null, make it "null".
 */
private static String formatServerName(HMaster master,
   ServerManager serverManager, ServerName serverName) {
  if (serverName == null) {
    return "null";
  }
  String sn = serverName.toString();
  if (serverManager.isServerOnline(serverName)) {
    int infoPort = master.getRegionServerInfoPort(serverName);
    if (infoPort > 0) {
      return "<a href=" + "//" + serverName.getHostname() + ":" +
        infoPort + "/rs-status>" + sn + "</a>";
    } else {
      return "<b>" + sn + "</b>";
    }
  } else if (serverManager.isServerDead(serverName)) {
    return "<i>" + sn + "</i>";
  }
  return sn;
}
%>
