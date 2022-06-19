from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

import os
import pandas as pd
from dotenv import load_dotenv
load_dotenv()


credentials_path = os.environ['GOOGLE_SHEET_CREDENTIALS']


def _create_gsheet_object(path_to_credentials, scopes):
    credentials = service_account.Credentials.from_service_account_file(
        path_to_credentials, scopes=scopes)
    service = build('sheets', 'v4', credentials=credentials)

    sheet = service.spreadsheets()
    return sheet


def get_sheets(spreadsheet_id, sheet=None, path_to_credentials=credentials_path):
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    if(sheet == None):
        sheet = _create_gsheet_object(path_to_credentials, scopes=SCOPES)

    return [x["properties"]["title"] for x in sheet.get(spreadsheetId=os.environ['GSHEET_DEFAULT']).execute()["sheets"]]


def create_sheet(spreadsheet_id, sheet_name, path_to_credentials=credentials_path, sheet=None):
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

    if(sheet == None):
        sheet = _create_gsheet_object(path_to_credentials, scopes=SCOPES)
    try:
        request_body = {
            'requests': [{
                'addSheet': {
                    'properties': {
                        'title': sheet_name
                    }
                }
            }]
        }

        response = sheet.batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=request_body
        ).execute()

        return response
    except Exception as e:
        print(e)


def read_df_from_gsheets(spreadsheet_id,
                         path_to_credentials=credentials_path,
                         sheet_name='Sheet1',
                         sheet_range=None,
                         first_row_is_header=True):
    '''Returns a Pandas DataFrame from Google Sheets, 
    for a given spreadsheet_id and range. Sheet must be shared 
    with the service account, e.g. :
    googlesheet@root-sanctuary-178203.iam.gserviceaccount.com'''

    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

    sheet = _create_gsheet_object(path_to_credentials, scopes=SCOPES)

    name_range = sheet_name + '!' + sheet_range if sheet_range else sheet_name
    result = sheet.values().get(spreadsheetId=spreadsheet_id,
                                range=name_range).execute()
    df = pd.DataFrame(result['values'])

    if first_row_is_header:
        df.columns = df.loc[0]
        df = df.drop(0)
    return df


def write_df_to_gsheets(df,
                        spreadsheet_id,
                        path_to_credentials=credentials_path,
                        sheet_name='Sheet1',
                        start_cell='A1',
                        include_column_names=True):
    '''Writes data from Pandas DataFrame to Google Sheets, 
    for a given spreadsheet_id, a sheet name and a start cell. 
    The service account must have write access to the sheet, e.g. :
    googlesheet@root-sanctuary-178203.iam.gserviceaccount.com'''

    SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
              'https://www.googleapis.com/auth/drive']

    sheet = _create_gsheet_object(path_to_credentials, scopes=SCOPES)

    if(sheet_name not in get_sheets(spreadsheet_id, sheet=sheet)):
        create_sheet(spreadsheet_id, sheet_name)

    name_range = sheet_name + '!' + start_cell

    values = []
    if include_column_names:
        values.extend([df.columns.values.tolist()])
    values.extend(df.values.tolist())
    data = {'values': values}

    response = sheet.values().update(spreadsheetId=spreadsheet_id,
                                     body=data,
                                     range=name_range,
                                     valueInputOption='RAW').execute()
    return response


def clear_values_of_gsheet(spreadsheet_id, sheet_range, path_to_credentials=credentials_path, sheet_name='Sheet1'):
    '''Clears cells in a Google Sheet, 
    for a given spreadsheet_id, and a sheet range 
    The service account must have write access to the sheet, e.g. :
    googlesheet@root-sanctuary-178203.iam.gserviceaccount.com'''

    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    sheet = _create_gsheet_object(path_to_credentials, scopes=SCOPES)

    name_range = sheet_name + '!' + sheet_range

    response = sheet.values().clear(
        spreadsheetId=spreadsheet_id, range=name_range).execute()

    return response
