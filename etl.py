import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential



@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def get_details(con_engine, google_sheet: str, tab: str):
    report_details = {}
    try:
        print(f"Getting Weekly Report Details from {google_sheet}")
        gsheet = con_engine.open(google_sheet)
        details = gsheet.worksheet(tab).get_all_values()
        for row in details[1:]:
            report_details[row[0]] = row[1]

        # Assigning values to variables    
        source = report_details['source']
        tabs_list = [item.strip() for item in report_details['tabs_list'].split(',')]
        destination = report_details['destination']
        destination_tab = report_details['destination_tab']
        start_date = report_details['start_date']
        end_date = report_details['end_date']
        destination_cell = report_details['destination_cell']

        return source, tabs_list, destination, destination_tab, start_date, end_date, destination_cell
    except Exception as e:
        print(f'Error Geting Data; {e}')





@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def extract(con_engine, google_sheet_name: str, tabs_list: list):

    data_accumulator = []
    try:
        print(f'Connecting to {google_sheet_name}')
        gsheet = con_engine.open(google_sheet_name)

        for i, tab in enumerate(tabs_list):

            print(f'      {i+1}/{len(tabs_list)}: Getting raw data from to {tab}')

            # Getting data from each tab
            tab_raw_data = gsheet.worksheet(tab).get_all_values()

            # Removing first 2 rows and selecting first 16 columns        
            raw_data_1 = [row[:12] for row in tab_raw_data[2:]]
                   
            # Adding the Google Sheet name and KOL Type
            if tab.startswith(('NON', 'Non', 'non', 'WOW', 'Wow', 'wow')):
                raw_data_2 = [ [google_sheet_name] + ['NONBR'] +row for row in raw_data_1 ]
            elif tab.startswith(('FF', 'ff', 'Ff')):
                raw_data_2 = [ [google_sheet_name] + ['FF'] +row for row in raw_data_1 ]
            else:
                raw_data_2 = [ [google_sheet_name] + ['BR'] +row for row in raw_data_1 ]
            
            data_accumulator.extend(raw_data_2)
        return data_accumulator
    except Exception as e:
        print(f"Error: {e}")





@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def transform (raw_data_list: list, headers: list, start_date, end_date):
    try:
        print(f'Transforming the Raw Data')        
        df = pd.DataFrame(raw_data_list, columns=headers)

        # Convert to datetime objects
        dates = pd.to_datetime(df['Publish Date'].str.strip(), format='%d/%m/%Y', errors='raise')

        # Convert back to string format and fill blanks
        df['Publish Date'] = dates.dt.strftime('%Y-%m-%d').fillna('')

      
        # Merging request name to df
        request_df = pd.read_csv('request-name.csv')
        df = df.merge(request_df, 'left', left_on=['Campaign Name', 'Request'], right_on=['Campaign', 'Request#'])

        mask = df['Request'].str.startswith(('WOW', 'FF'))
        df.loc[~mask, 'Request'] = df['Request'] + ' - ' + df['Req-Name']

        # Select a Specific Date range
        mask_2 = df['Publish Date'].between(start_date, end_date)
        df = df[mask_2]
        
        target_cols = [
            'KOL Type', 'Name', 'Request', 'Video link', 'Type', 
            'Platform', 'Publish Date', 'Views', 'Likes', 'Comments', 'Trending'
        ]

        return df[target_cols]
    except Exception as e:
        print(f"Error Parsing the file...\n{e}")





@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def load(con_engine, clean_dataframe: pd.DataFrame, google_sheet_name: str, tab_name: str, cell: str):
    try:
    # Connecting to Google sheet    
        sh = con_engine.open(google_sheet_name)

        # Loading Data
        print(f'Loading Clean Data to {google_sheet_name} Sheet...')
        wrksht = sh.worksheet(tab_name)
        wrksht.clear()
        final_data_to_upload = [[col for col in clean_dataframe.columns]] + clean_dataframe.values.tolist()
        wrksht.update(values=final_data_to_upload, value_input_option='user_entered', range_name=cell)
        print('Success: Data Loaded to destination!!!')
    
    except Exception as e:
        print(f'Error loading data to {google_sheet_name}: {e}')