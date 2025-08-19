import requests
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# === CONFIG ===
TENANT_ID = ""
CLIENT_ID = ""
CLIENT_SECRET = ""
WORKSPACE_ID = ""
DATASET_ID = ""
USERNAME = ""
PASSWORD = ""
DAX_QUERY = r"""
DEFINE
    VAR _table =
        FILTER(
            SUMMARIZECOLUMNS(
                master_SC[Reg Short],
                master_SC[NIK SC],
                master_SC[UNIQ SC],
                master_SC[NAME SC],
                master_SC[Brand],
                'calendar lite 2'[Year],
                "Total BO", [BO Achv]
            ),
            'calendar lite 2'[Year] = 2025 && master_SC[Reg Short] = "R1"
        )
EVALUATE
    SELECTCOLUMNS(
        _table,
        "Region", master_SC[Reg Short],
        "NIK SC", master_SC[NIK SC],
        "UNIQ SC", master_SC[UNIQ SC],
        "SC NAME", master_SC[NAME SC],
        "BO Achievement", [Total BO]
    )
"""

# Google Sheets
#SPREADSHEET_ID = ""
SPREADSHEET_ID = ""

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
    creds = ServiceAccountCredentials.from_json_keyfile_name(r"C", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet('Push PBI')

    # Clear & updatedata
    data_rows = data
    header = ["Region", "NIK SC", "UNIQ SC", "SC Name", "BO Achievement"]
    final_data = [header] + data_rows
    sheet.clear()
    sheet.update(range_name="A1", values=final_data)

# === MAIN ===
if __name__ == '__main__':
    token = get_access_token()
    rows = execute_dax_query(token)
    formatted_data = [list(row.values()) for row in rows]
    push_to_google_sheets(formatted_data)

    print('✅ Data pushed to Google Sheets!')
