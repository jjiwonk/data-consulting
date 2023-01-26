from spreadsheet import spreadsheet

url = 'https://docs.google.com/spreadsheets/d/1w23AdEYLf4OJDlTElf2vwbVPttctSx-2nB51ZHQDEJQ/edit#gid=1992691648'
doc = spreadsheet.spread_document_read(url)

class sheet :
    owner_id_sheet = spreadsheet.spread_sheet(doc, 'Owner 정보')
    client_sheet = spreadsheet.spread_sheet(doc, '광고주 정보')
    campaign_sheet = spreadsheet.spread_sheet(doc, '캠페인 정보')
    campaign_sheet = campaign_sheet.loc[campaign_sheet['광고주']!='']


def get_owner_info():
    owner_info = sheet.owner_id_sheet.drop_duplicates('Owner ID')
    owner_info = owner_info.rename(columns={'Owner ID': 'owner_id'})
    owner_category = sheet.client_sheet.drop_duplicates('광고주', keep='last')

    owner_info_merge = owner_info.merge(owner_category, on='광고주')
    return owner_info_merge
