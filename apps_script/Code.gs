/**
 * Google Apps Script web app that accepts POST requests with data
 * and writes to the appropriate sheet tabs.
 *
 * Setup:
 *   1. Open your Google Sheet
 *   2. Extensions -> Apps Script
 *   3. Paste this entire file into the editor (replacing any existing code)
 *   4. Deploy -> New deployment -> Web app
 *      - Execute as: Me
 *      - Who has access: Anyone
 *   5. Copy the deployment URL into your .env as APPS_SCRIPT_WEBHOOK_URL
 *
 * Expected POST body (JSON):
 *   {
 *     "action": "write" | "append" | "clear" | "get_watermark",
 *     "tab": "Sheet tab name",
 *     "headers": ["col1", "col2", ...],       // for write/append
 *     "rows": [["val1", "val2"], ...],         // for write/append
 *     "secret": "optional shared secret"       // if you set WEBHOOK_SECRET
 *   }
 */

// Optional: set a shared secret to prevent unauthorized writes.
// Leave empty string to skip auth check.
var WEBHOOK_SECRET = "";

function doPost(e) {
  try {
    var payload = JSON.parse(e.postData.contents);

    // Optional secret check
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

function doGet(e) {
  return _jsonResponse({ status: "ok", message: "POST data to this endpoint" });
}

// ---- Action handlers ----

function _handleWrite(ws, payload) {
  var headers = payload.headers || [];
  var rows = payload.rows || [];

  ws.clear();

  if (headers.length > 0) {
    ws.getRange(1, 1, 1, headers.length).setValues([headers]);
    // Bold headers and freeze
    ws.getRange(1, 1, 1, headers.length).setFontWeight("bold");
    ws.setFrozenRows(1);
  }

  if (rows.length > 0) {
    // Write in batches of 500 to stay within execution limits
    var startRow = 2;
    var batchSize = 500;
    for (var i = 0; i < rows.length; i += batchSize) {
      var batch = rows.slice(i, i + batchSize);
      var numCols = batch[0].length;
      ws.getRange(startRow + i, 1, batch.length, numCols).setValues(batch);
    }
  }

  return _jsonResponse({
    status: "written",
    tab: ws.getName(),
    rows_written: rows.length
  });
}

function _handleAppend(ws, payload) {
  var headers = payload.headers || [];
  var rows = payload.rows || [];

  if (rows.length === 0) {
    return _jsonResponse({ status: "no_rows", tab: ws.getName(), rows_appended: 0 });
  }

  var lastRow = ws.getLastRow();

  // If sheet is empty, write headers first
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

  return _jsonResponse({
    status: "appended",
    tab: ws.getName(),
    rows_appended: rows.length
  });
}

function _handleGetWatermark(ws) {
  var lastRow = ws.getLastRow();
  if (lastRow <= 1) {
    return _jsonResponse({ watermark: null, tab: ws.getName() });
  }
  // Column A, last row
  var watermark = ws.getRange(lastRow, 1).getValue();
  return _jsonResponse({
    watermark: watermark ? watermark.toString() : null,
    tab: ws.getName()
  });
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
