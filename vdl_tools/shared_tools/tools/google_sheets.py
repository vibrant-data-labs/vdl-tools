import pandas as pd

import json
import os
import pickle

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

from vdl_tools.shared_tools.tools.config_utils import get_configuration


SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
]


def get_sheets_service(credentials_json=None):

    config = get_configuration()
    if not credentials_json:
        credentials_json = json.loads(config.get('google_sheets', 'credentials'))

    creds = None
    # token.pickle stores the user's access and refresh tokens. 
    # It's automatically created when the authorization flow completes for the first time.
    if os.path.exists('google_sheets_token.pickle'):
        with open('google_sheets_token.pickle', 'rb') as token:
            creds = pickle.load(token)

    # If there are no valid credentials or the credentials are expired, let's log in again.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # "credentials.json" is the file you downloaded from the Google Cloud Console
            flow = InstalledAppFlow.from_client_config(credentials_json, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('google_sheets_token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    # Build the service
    service = build('sheets', 'v4', credentials=creds)
    return service


def ensure_sheet_exists(service, spreadsheet_id, sheet_name):
    # Get current sheet titles
    meta = service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        fields='sheets(properties(title))'
    ).execute()

    titles = {s['properties']['title'] for s in meta['sheets']}
    if sheet_name in titles:
        return  # already there

    # Add the sheet
    body = {
        "requests": [
            {"addSheet": {"properties": {"title": sheet_name}}}
        ]
    }
    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body=body
    ).execute()


def get_sheet_values(
    spreadsheet_id,
    service=None,
    range_value=None,
    sheet_name=None,
    first_row_header=True,
):
    if not service:
        service = get_sheets_service()

    if range_value and sheet_name:
        raise ValueError('Cannot provide both range_value and sheet_name')

    if sheet_name:
        range_value = f"'{sheet_name}'!A1:Z"
    else:
        range_value = range_value or 'A1:Z'

    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_value).execute()
    values = result.get('values', [])
    if first_row_header:
        headers = values[0]
        values = values[1:]
        df = pd.DataFrame(values, columns=headers)
    else:
        df = pd.DataFrame(values)
    return df


def write_to_sheet(
    spreadsheet_id: str,
    values,
    sheet_name: str,
    service=None,
    mode: str = "overwrite",        # "overwrite" | "append"
    include_header: bool | None = None,
):
    if service is None:
        service = get_sheets_service()

    ensure_sheet_exists(service, spreadsheet_id, sheet_name)

    # ── decide default for include_header ───────────────────────────────────
    if include_header is None:
        if mode == "overwrite":
            include_header = True
        else:  # mode == "append" → auto: header ONLY if sheet is empty
            resp = service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"'{sheet_name}'!A1:A1",
                majorDimension="ROWS"
            ).execute()
            sheet_is_empty = not resp.get("values")
            include_header = sheet_is_empty

    # ── normalise DataFrame / list input ───────────────────────────────────
    if isinstance(values, pd.DataFrame):
        if isinstance(values, pd.DataFrame):
            # Convert DataFrame to list, replacing NaN with None for JSON compliance
            df_clean = values.reset_index(drop=True).apply(lambda col: col.map(lambda x: None if pd.isna(x) else x))
            rows = df_clean.values.tolist()
        if include_header:
            rows = [values.columns.tolist()] + rows
        values = rows
    elif not isinstance(values, list):
        raise TypeError("'values' must be a DataFrame or list-of-lists")

    if not values:
        return {"status": "nothing to write"}

    sheet = service.spreadsheets()

    if mode == "overwrite":
        sheet.values().clear(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'"
        ).execute()

        r, c = len(values), len(values[0])
        last_col = _col_letter(c)
        target = f"'{sheet_name}'!A1:{last_col}{r}"

        return sheet.values().update(
            spreadsheetId=spreadsheet_id,
            range=target,
            valueInputOption="USER_ENTERED",
            body={"values": values},
        ).execute()

    elif mode == "append":
        last_col = _col_letter(len(values[0]))
        target = f"'{sheet_name}'!A:{last_col}"

        return sheet.values().append(
            spreadsheetId=spreadsheet_id,
            range=target,
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": values},
        ).execute()

    else:
        raise ValueError("mode must be 'overwrite' or 'append'")


def _col_letter(n: int) -> str:
    """1-based number → column letter, supports >26."""
    s = ""
    while n:
        n, rem = divmod(n - 1, 26)
        s = chr(65 + rem) + s
    return s
