// === CONFIG ===
const CONFIG = {
  TENANT_ID: "578dbadd-c32c-4f5a-8c44-33da44c71209",
  CLIENT_ID: "26fd9846-9c01-45c2-b3f0-75f746475010",
  CLIENT_SECRET: "S2L8Q~dkhYRTzmyEBD8IB9~6D61XjzsnGaD3mbJn",
  WORKSPACE_ID: "d6efca8d-5843-4c2c-9299-36b6d3194d26",
  DATASET_ID: "bcace2f2-f1fd-48ba-be8b-8830467d95dc",
  USERNAME: "pbi_administrator@teletamaID.onmicrosoft.com",
  PASSWORD: "Satusatu11!",
  SHEET_NAME: "target_breakdown"
};

const DAX_QUERY = `
EVALUATE
  CALCULATETABLE(
      SELECTCOLUMNS(
          SUMMARIZECOLUMNS(
              'Target BP Rofo'[Date],
              master_region[Region],
              master_brand[Brand 2b],
              "Target BO", [Total Target BO]
          ),
          "Period", 'Target BP Rofo'[Date],
          "Region", master_region[Region],
          "Brand", master_brand[Brand 2b],
          "Total Target BO", [Total Target BO]
      ),
      'Target BP Rofo'[Date] > DATE(2025,12,31)
)
`;

// === MAIN FUNCTION ===
function mainPBIToGS() {
  try {
    Logger.log("=== Starting PBI to Google Sheets ===");
    const token = getAccessToken();
    Logger.log("✅ Token received");
    const rows = executeDaxQuery(token);
    Logger.log("✅ DAX query executed, received " + rows.length + " rows");
    pushToSheet(rows);
    Logger.log("✅ Data pushed to Google Sheets!");
  } catch (e) {
    Logger.log("❌ Error: " + e.toString());
    Logger.log("Stack: " + e.stack);
  }
}

// === Power BI Auth ===
function getAccessToken() {
  const url = `https://login.microsoftonline.com/${CONFIG.TENANT_ID}/oauth2/token`;
  
  const payload = {
    'resource': 'https://analysis.windows.net/powerbi/api',
    'client_id': CONFIG.CLIENT_ID,
    'client_secret': CONFIG.CLIENT_SECRET,
    'grant_type': 'password',
    'username': CONFIG.USERNAME,
    'password': CONFIG.PASSWORD,
    'scope': 'openid'
  };

  // Convert to form-encoded string
  const formData = Object.keys(payload)
    .map(key => encodeURIComponent(key) + '=' + encodeURIComponent(payload[key]))
    .join('&');

  const options = {
    'method': 'post',
    'headers': {
      'Content-Type': 'application/x-www-form-urlencoded'
    },
    'payload': formData,
    'muteHttpExceptions': true
  };

  const response = UrlFetchApp.fetch(url, options);
  const responseText = response.getContentText();
  const statusCode = response.getResponseCode();
  
  Logger.log("Token Response Status: " + statusCode);
  
  if (statusCode !== 200) {
    throw new Error("Auth failed with status " + statusCode + ": " + responseText);
  }
  
  const resJson = JSON.parse(responseText);
  
  if (resJson.access_token) {
    return resJson.access_token;
  } else {
    throw new Error("Failed to get token: " + responseText);
  }
}

// === Execute DAX Query ===
function executeDaxQuery(token) {
  const url = `https://api.powerbi.com/v1.0/myorg/groups/${CONFIG.WORKSPACE_ID}/datasets/${CONFIG.DATASET_ID}/executeQueries`;
  
  if (!token) {
    throw new Error("Token is undefined or null. getAccessToken() failed to return a valid token.");
  }
  
  Logger.log("Fetching from URL: " + url);
  Logger.log("Workspace ID: " + CONFIG.WORKSPACE_ID);
  Logger.log("Dataset ID: " + CONFIG.DATASET_ID);
  Logger.log("Token (first 20 chars): " + token.substring(0, 20) + "...");
  
  const payload = {
    "queries": [{ "query": DAX_QUERY }],
    "serializerSettings": { "includeNulls": true }
  };

  const options = {
    'method': 'post',
    'headers': {
      'Authorization': 'Bearer ' + token,
      'Content-Type': 'application/json'
    },
    'payload': JSON.stringify(payload),
    'muteHttpExceptions': true
  };

  const response = UrlFetchApp.fetch(url, options);
  const responseText = response.getContentText();
  const statusCode = response.getResponseCode();
  
  Logger.log("Response Status: " + statusCode);
  Logger.log("Response Body: " + responseText);
  
  if (statusCode !== 200) {
    throw new Error("API returned status " + statusCode + ": " + responseText);
  }
  
  if (!responseText || responseText.trim() === "") {
    throw new Error("Empty response from Power BI API");
  }
  
  const result = JSON.parse(responseText);
  
  if (result.results && result.results[0].tables[0].rows) {
    return result.results[0].tables[0].rows;
  } else {
    throw new Error("Query failed - unexpected response structure: " + responseText);
  }
}

// === Push to Google Sheets ===
function pushToSheet(rows) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(CONFIG.SHEET_NAME);
  
  if (!sheet) {
    sheet = ss.insertSheet(CONFIG.SHEET_NAME);
  }


  const header = ["[Period]", "[Region]", "[Brand]", "[Total Target BO]"];
  const displayHeader = ["Period", "Region", "Brand", "Total Target BO"];
  
  Logger.log("Total rows received: " + rows.length);
  Logger.log("First row raw: " + JSON.stringify(rows[0]));
  
  const finalData = rows.map((row, index) => {
    const rowData = header.map(h => {
      const value = row[h] !== undefined ? row[h] : "";
      if (index === 0) {
        Logger.log("Row 0, Column " + h + ": " + value);
      }
      return value;
    });
    return rowData;
  });

  finalData.unshift(displayHeader);
  
  Logger.log("Final data to insert: " + finalData.length + " rows");
  Logger.log("Final data sample: " + JSON.stringify(finalData.slice(0, 3)));

  sheet.getRange("A:D").clearContent();
  if (finalData.length > 1) {
    sheet.getRange(1, 1, finalData.length, header.length).setValues(finalData);
    Logger.log("✅ Data inserted into sheet");
  } else {
    Logger.log("⚠️ No data rows to insert (only header)");
  }
}