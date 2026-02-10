import gspread
from etl import extract, transform, load

con_engine = gspread.service_account(filename='goolglesheet-connect.json')
source = 'January+ February 2026 | PUBGM'
destination = 'Intermediate'
destination_tab = 'jan_feb'
start_date = '2026-02-01'
end_date = '2026-02-10'

tabs_list = ['NonBr WOW Request', 'Request 0', 'Request 1', 'Request 2', 'Request 3', 
                    'Request 4', 'Request 6', 'Request 7','Request 8', 'Request 9', 'Request 10', 
                    'Request 11','Request 12', 'Request 13', 'Request 14',  'Request 15',  'Request 16',
                    'Request 17','Request 18', 'Request 19', 'Request 20',  'Request 21',  'Request 22',
                     'Request 23', 'FF January', 'FF February'
                    ]

columns = ['Campaign Name', 'KOL Type', 'Name', 'Request', 'Video link', 'Type', 'Platform', 'Approval', 
           'Publish Date', 'Views', 'Likes', 'Comments', 'Screenshot', 'Trending']

def main():
    try:
        raw_data = extract(
            con_engine=con_engine,
            google_sheet_name=source,
            tabs_list=tabs_list
            )

        clean_data = transform(
            raw_data_list=raw_data,
            headers=columns,
            start_date=start_date,
            end_date=end_date
            )
        
        load(
            con_engine=con_engine,
            clean_dataframe=clean_data,
            google_sheet_name=destination,
            tab_name=destination_tab
        )
    except Exception as e:
        print(f'Error: {e}')