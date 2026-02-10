import gspread
from etl import get_details, extract, transform, load

con_engine = gspread.service_account(filename='goolglesheet-connect.json')
google_sheet_name = 'Intermediate'
tab = 'report_details'

columns = ['Campaign Name', 'KOL Type', 'Name', 'Request', 'Video link', 'Type', 'Platform', 'Approval', 
           'Publish Date', 'Views', 'Likes', 'Comments', 'Screenshot', 'Trending']

def main():
    try:
        source, tabs_list, destination, destination_tab, start_date, end_date, destination_cell = get_details(
                                                                                                        con_engine, 
                                                                                                        google_sheet_name, 
                                                                                                        tab
                                                                                                        )

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
            tab_name=destination_tab,
            cell=destination_cell
        )
    except Exception as e:
        print(f'Error: {e}')

if __name__ == '__main__':
    main()