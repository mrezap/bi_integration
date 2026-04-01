import requests
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import datetime

# === CONFIG ===
TENANT_ID = "578dbadd-c32c-4f5a-8c44-33da44c71209"
CLIENT_ID = "26fd9846-9c01-45c2-b3f0-75f746475010"
CLIENT_SECRET = "S2L8Q~dkhYRTzmyEBD8IB9~6D61XjzsnGaD3mbJn"
WORKSPACE_ID = "d6efca8d-5843-4c2c-9299-36b6d3194d26"
DATASET_ID = "bcace2f2-f1fd-48ba-be8b-8830467d95dc"
USERNAME = "pbi_administrator@teletamaID.onmicrosoft.com"
PASSWORD = "Satusatu11!"
DAX_QUERY = r"""
DEFINE
    VAR _maxDate = MAX('calendar lite 2'[Date])
    VAR _minDate = DATE(YEAR(_maxDate), MONTH(_maxDate), 1)
    VAR _period = 
        TREATAS(VALUES('calendar lite 2'[Date]), 'calendar lite 2'[Date])

    VAR _filteredTable =
        FILTER(
            SUMMARIZECOLUMNS(
                'master_BP'[Dealer Full],
                'master_SC'[UNIQ SC],
                'master_SC'[NAME SC],
                'master_SC'[Brand],
                'master_SC'[Date],
                'calendar lite 2'[Month-Year],
                _period,
                "TTL_Target_Rofo", 'Metrics'[TTL Target Rofo],
                "Total_Nett_No_Tax", 'Metrics'[Total Nett No Tax],
                "QVO_Potential", 'Metrics'[QVO Potential]
            ),
            'master_SC'[Date] >= _minDate &&
            'master_SC'[Date] <= _maxDate &&
			NOT ISBLANK( [TTL_Target_Rofo] )
        )

EVALUATE
SELECTCOLUMNS(
    _filteredTable,
    "Period", [Month-Year],
    "SC Uniqcode", [UNIQ SC],
    "Brand", [Brand],
    "Dealer", [Dealer Full],
    "Total Target", [TTL_Target_Rofo],
    "Total Sales", IF( ISBLANK( [Total_Nett_No_Tax] ), 0 ,[Total_Nett_No_Tax] ),
    "QVO Potential", IF( ISBLANK( [QVO_Potential] ), 0 , [QVO_Potential] )
)
"""

# Google Sheets
#SPREADSHEET_ID = "1MlGmXqIKfHMh6J15-ncWKLEYLZUdBGOHQ6KoesmslF8"
SPREADSHEET_ID = "1E8H4Cgmtfo2NOFoXnnb-5lOdZQBjwgy7tJdXc2t5HVo"

#=== STEP 1: Auth to Power BI ===
def get_access_token():
    url = f'https://login.microsoftonline.com/{TENANT_ID}/oauth2/token'
    resource = 'https://analysis.windows.net/powerbi/api'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    payload = {
        'resource': resource,
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'grant_type': 'password',
        'username': USERNAME,
        'password': PASSWORD,
        'scope': 'openid'
    }
    # data = {
    #     'grant_type': 'client_credentials',
    #     'client_id': CLIENT_ID,
    #     'client_secret': CLIENT_SECRET,
    #     'scope': 'https://analysis.windows.net/powerbi/api/.default'
    # }
    response = requests.post(url, headers=headers, data=payload)
    return response.json()['access_token']

# === STEP 2: Execute DAX Query ===
def execute_dax_query(token):
    print("Access Token:", token[:20], "...")
    url = f'https://api.powerbi.com/v1.0/myorg/groups/{WORKSPACE_ID}/datasets/{DATASET_ID}/executeQueries'
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    payload = {
        "queries": [
            {
                "query": DAX_QUERY
            }
        ],
        #"impersonatedUserName": "powerbi.CM_B2B@teletamaID.onmicrosoft.com",
        "serializerSettings": {
            "includeNulls": True
        }
    }
    response = requests.post(url, headers=headers, json=payload)
    print("Status code:", response.status_code)
    result = response.json()
    # print("Full response:")
    # print(json.dumps(result, indent=2))
    rows = result['results'][0]['tables'][0]['rows']
    return rows

# === STEP 3: Push to Google Sheets ===
def push_to_google_sheets(data):
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(r"C:\Users\User\Videos\pbi-gs-cred.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet('dealer_target')

    # Clear & updatedata
    data_rows = data
    #header = ["Region", "NIK SC", "UNIQ SC", "SC Name", "BO Achievement"]
    header = ["Period", "UNIQ SC", "Brand", "Dealer", "Target Rofo", "Total Sales", "QVO Potential", "update_at"]
    final_data = [header] + data_rows
    sheet.clear()
    sheet.update(range_name="A1", values=final_data)

# === MAIN ===
if __name__ == '__main__':
    token = get_access_token()
    rows = execute_dax_query(token)
    formatted_data = [list(row.values()) for row in rows]

    # Get Pushed Time
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with_timestamp = []
    for row in formatted_data:
        new_row = row + [current_time]
        with_timestamp.append(new_row)
    
    push_to_google_sheets(with_timestamp)
    print('✅ Data pushed to Google Sheets!')