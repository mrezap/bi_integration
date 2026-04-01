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
	VAR _table =
		VAR _maxDate = MAX ( 'calendar lite 2'[Date] )
		VAR _minDate = DATE( YEAR( _maxDate ), MONTH(_maxDate ), 1)
		RETURN
			ADDCOLUMNS(
				SUMMARIZE( master_SC, master_SC[UNIQ ASH], master_SC[NAME ASH], master_SC[UNIQ SC], master_SC[NAME SC], master_SC[Date] ),
                "Target Rofo", CALCULATE(
					[TTL Target Rofo],
					DATESBETWEEN( 'calendar lite 2'[Date], _minDate, _maxDate )
				),
				"Total Sales", CALCULATE(
					[Total Nett No Tax],
					DATESBETWEEN( 'calendar lite 2'[Date], _minDate, _maxDate )
				),
				"Tgt BO", CALCULATE(
					[Total Target BO],
					DATESBETWEEN( 'calendar lite 2'[Date], _minDate, _maxDate )
				),
				"Acv BO", CALCULATE(
					[BO Achv],
					DATESBETWEEN( 'calendar lite 2'[Date], _minDate, _maxDate )
				),
				"Tgt QVO", CALCULATE(
					[Total Target QVO],
					DATESBETWEEN( 'calendar lite 2'[Date], _minDate, _maxDate )
				),
				"Acv QVO", CALCULATE(
					[QVO Achv],
					DATESBETWEEN( 'calendar lite 2'[Date], _minDate, _maxDate )
				)
			)
EVALUATE
	FILTER(
		SELECTCOLUMNS(
			_table,
			"Period", IF( NOT ISBLANK( [Total Sales] ), FORMAT([Date], "MMM-yyyy" ) ),
			"ASH Uniqcode", [UNIQ ASH],
            "SC Uniqcode", [UNIQ SC],
            "Target Rofo", IF( ISBLANK( [Target Rofo] ), 0, [Target Rofo] ),
			"Total Sales", [Total Sales],
			"Target BO", IF( ISBLANK( [Tgt BO] ), 0, [Tgt BO] ),
			"Actual BO", IF( ISBLANK( [Acv BO] ), 0, [Acv BO] ),
			"Target QVO", IF( ISBLANK( [Tgt QVO] ), 0, [Tgt QVO] ),
			"Actual QVO", IF( ISBLANK( [Acv QVO] ), 0, [Acv QVO] )
		),
        NOT ISBLANK( [Period] )
	)
"""
# DEFINE
#     VAR _table =
#         FILTER(
#             SUMMARIZECOLUMNS(
#                 master_SC[Reg Short],
#                 master_SC[NIK SC],
#                 master_SC[UNIQ SC],
#                 master_SC[NAME SC],
#                 master_SC[Brand],
#                 'calendar lite 2'[Year],
#                 "Total BO", [BO Achv]
#             ),
#             'calendar lite 2'[Year] = 2025 && master_SC[Reg Short] = "R1"
#         )
# EVALUATE
#     SELECTCOLUMNS(
#         _table,
#         "Region", master_SC[Reg Short],
#         "NIK SC", master_SC[NIK SC],
#         "UNIQ SC", master_SC[UNIQ SC],
#         "SC NAME", master_SC[NAME SC],
#         "BO Achievement", [Total BO]
#     )

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
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet('data_achv')

    # Clear & updatedata
    data_rows = data
    #header = ["Region", "NIK SC", "UNIQ SC", "SC Name", "BO Achievement"]
    header = ["Period", "UNIQ ASH", "UNIQ SC", "Target Rofo", "Total Sales", "Target BO", "Achv BO","Target QVO", "Achv QVO", "update_at"]
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