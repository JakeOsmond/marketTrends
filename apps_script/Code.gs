/**
 * Google Apps Script web app — dual purpose:
 *   1. POST endpoint: receives data from the Python pipeline
 *   2. GET endpoint: serves the HX Insurance Pulse dashboard HTML
 *
 * Setup:
 *   1. Open your Google Sheet
 *   2. Extensions -> Apps Script
 *   3. Paste this file as Code.gs
 *   4. Create a new HTML file called "Dashboard" and paste Dashboard.html
 *   5. Deploy -> New deployment -> Web app
 *      - Execute as: Me
 *      - Who has access: Anyone (within Holiday Extras domain)
 *   6. Copy the deployment URL:
 *      - Into your .env as APPS_SCRIPT_WEBHOOK_URL (for pipeline writes)
 *      - Into Google Sites as the embed URL (for the dashboard)
 *
 * IMPORTANT: After updating code, create a NEW deployment version
 * (Deploy -> Manage deployments -> Edit -> New version)
 */

var WEBHOOK_SECRET = "";

// ======================================================================
// GET — Serve the dashboard
// ======================================================================
function doGet(e) {
  var template = HtmlService.createTemplateFromFile('Dashboard');
  template.dashboardData = JSON.stringify(getAllDashboardData());
  return template.evaluate()
    .setTitle('HX Insurance Pulse')
    .addMetaTag('viewport', 'width=device-width, initial-scale=1')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

function getAllDashboardData() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  return {
    weekly: _readTab(ss, 'Dashboard Weekly'),
    metrics: _readTabAsMap(ss, 'Dashboard Metrics', 'metric_key'),
    aiInsights: _readTabAsMap(ss, 'AI Insights', 'section_key'),
    quarterly: _readTab(ss, 'Market Demand Summary'),
    freshness: _readTab(ss, 'Data Freshness'),
    sectionTrends: _readTab(ss, 'Dashboard Section Trends'),
    competitors: _readTab(ss, 'Dashboard Competitors'),
    channels: _readTab(ss, 'Dashboard Channels'),
  };
}

function _readTab(ss, tabName) {
  var ws = ss.getSheetByName(tabName);
  if (!ws) return [];
  var data = ws.getDataRange().getValues();
  if (data.length < 2) return [];
  var headers = data[0];
  var rows = [];
  for (var i = 1; i < data.length; i++) {
    var row = {};
    for (var j = 0; j < headers.length; j++) {
      var val = data[i][j];
      // Convert Date objects to ISO strings
      if (val instanceof Date) {
        val = Utilities.formatDate(val, Session.getScriptTimeZone(), "yyyy-MM-dd");
      }
      row[headers[j]] = val;
    }
    rows.push(row);
  }
  return rows;
}

function _readTabAsMap(ss, tabName, keyCol) {
  var rows = _readTab(ss, tabName);
  var map = {};
  for (var i = 0; i < rows.length; i++) {
    var key = rows[i][keyCol];
    if (key) map[key] = rows[i];
  }
  return map;
}

// ======================================================================
// POST — Receive data from pipeline
// ======================================================================
function doPost(e) {
  try {
    var payload = JSON.parse(e.postData.contents);

    if (WEBHOOK_SECRET && payload.secret !== WEBHOOK_SECRET) {
      return _jsonResponse({ error: "unauthorized" }, 403);
    }

    var action = payload.action;
    var tabName = payload.tab;

    if (!tabName) {
      return _jsonResponse({ error: "missing 'tab' field" }, 400);
    }

    var ss = SpreadsheetApp.getActiveSpreadsheet();
    var ws = _getOrCreateSheet(ss, tabName);

    switch (action) {
      case "write":
        return _handleWrite(ws, payload);
      case "append":
        return _handleAppend(ws, payload);
      case "clear":
        ws.clear();
        return _jsonResponse({ status: "cleared", tab: tabName });
      case "get_watermark":
        return _handleGetWatermark(ws);
      default:
        return _jsonResponse({ error: "unknown action: " + action }, 400);
    }
  } catch (err) {
    return _jsonResponse({ error: err.toString() }, 500);
  }
}

// ---- Action handlers ----

function _handleWrite(ws, payload) {
  var headers = payload.headers || [];
  var rows = payload.rows || [];

  ws.clear();

  if (headers.length > 0) {
    ws.getRange(1, 1, 1, headers.length).setValues([headers]);
    ws.getRange(1, 1, 1, headers.length).setFontWeight("bold");
    ws.setFrozenRows(1);
  }

  if (rows.length > 0) {
    var startRow = 2;
    var batchSize = 500;
    for (var i = 0; i < rows.length; i += batchSize) {
      var batch = rows.slice(i, i + batchSize);
      var numCols = batch[0].length;
      ws.getRange(startRow + i, 1, batch.length, numCols).setValues(batch);
    }
  }

  return _jsonResponse({ status: "written", tab: ws.getName(), rows_written: rows.length });
}

function _handleAppend(ws, payload) {
  var headers = payload.headers || [];
  var rows = payload.rows || [];

  if (rows.length === 0) {
    return _jsonResponse({ status: "no_rows", tab: ws.getName(), rows_appended: 0 });
  }

  var lastRow = ws.getLastRow();

  if (lastRow === 0 && headers.length > 0) {
    ws.getRange(1, 1, 1, headers.length).setValues([headers]);
    ws.getRange(1, 1, 1, headers.length).setFontWeight("bold");
    ws.setFrozenRows(1);
    lastRow = 1;
  }

  var startRow = lastRow + 1;
  var batchSize = 500;
  for (var i = 0; i < rows.length; i += batchSize) {
    var batch = rows.slice(i, i + batchSize);
    var numCols = batch[0].length;
    ws.getRange(startRow + i, 1, batch.length, numCols).setValues(batch);
  }

  return _jsonResponse({ status: "appended", tab: ws.getName(), rows_appended: rows.length });
}

function _handleGetWatermark(ws) {
  var lastRow = ws.getLastRow();
  if (lastRow <= 1) {
    return _jsonResponse({ watermark: null, tab: ws.getName() });
  }
  var watermark = ws.getRange(lastRow, 1).getValue();
  return _jsonResponse({ watermark: watermark ? watermark.toString() : null, tab: ws.getName() });
}

// ---- Helpers ----

function _getOrCreateSheet(ss, tabName) {
  var ws = ss.getSheetByName(tabName);
  if (!ws) {
    ws = ss.insertSheet(tabName);
  }
  return ws;
}

function _jsonResponse(data, code) {
  return ContentService
    .createTextOutput(JSON.stringify(data))
    .setMimeType(ContentService.MimeType.JSON);
}
