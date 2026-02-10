# 🚀 Weekly Report ETL Automation

A Python-based ETL (Extract, Transform, Load) pipeline that automates report generation by fetching data from Google Sheets, transforming it with Pandas, and loading the cleaned results into a destination dashboard.

## 🛠 Features
* **Automated Extraction:** Pulls raw data from multiple Google Sheet tabs based on a configuration list.
* **Smart Categorization:** Uses `startswith` logic to automatically assign **KOL Types** (NONBR, FF, BR) based on tab names.
* **Data Cleaning:** Normalizes dates, merges campaign metadata from `request-name.csv`, and filters results by date range.
* **Resilience:** Built-in retry logic using `tenacity` to handle API rate limits and network blips.
* **GitHub Actions Integrated:** Runs on a schedule or can be triggered via a custom button in Google Sheets.

## 🏗 Project Structure
* `main.py`: The entry point that orchestrates the ETL flow.
* `etl.py`: Contains the core logic for Extract, Transform, and Load functions.
* `request-name.csv`: Lookup file for mapping Campaign and Request IDs to friendly names (managed via GitHub Secrets).
* `.github/workflows/main.yml`: Configuration for automated runs and environment setup.

## ⚙️ Setup & Deployment
1. **Secrets:** Store your `goolglesheet-connect.json` and `request-name.csv` content in GitHub Repository Secrets as `GCP_SERVICE_ACCOUNT` and `REQUEST_NAME_CSV`.
2. **Configuration:** Update the `Intermediate` sheet and `report_details` tab to specify source tabs and date ranges.
3. **Trigger:**
    * **Manual:** Use the "🚀 ETL Automation" menu in Google Sheets (via Apps Script).

## 📦 Dependencies
* `pandas`: For data manipulation and analysis.
* `gspread`: For Google Sheets API interaction.
* `tenacity`: For robust error handling and retries.
