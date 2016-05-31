import gspread
from oauth2client.service_account import ServiceAccountCredentials


def get_spread_sheet_data(spread_sheet_id):
    """
    :param spread_sheet_id: str
    :return: google spread_sheet
    """
    scopes = ['https://spreadsheets.google.com/feeds/']
    credentials = ServiceAccountCredentials.from_json_keyfile_name('app/credentials/extractor-3fb583056fd4.json',
                                                                   scopes)
    google_credentials = gspread.authorize(credentials)
    spread_sheet = google_credentials.open_by_key(spread_sheet_id)
    return spread_sheet
