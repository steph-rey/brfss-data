# Purpose: Generate and run function to fetch filtered BRFSS data from API endpoint 
# Last modified: 2024.12.14

# Import required packages 
import requests
import pandas as pd
from sqlalchemy import create_engine

# Define API details
API_URL = "https://data.cdc.gov/resource/dttw-5yxu.json"  # Socrata API endpoint
APP_TOKEN = "DVDdMVBH0t9Ausqgj6IxbvZha"  # Optional: Use if API requires authentication

# Function to fetch filtered data
def fetch_all_data(api_url, app_token=None, max_records=2_760_000, batch_size=50_000):
    """
    Fetch all rows from the API where Locationabbr = 'US', using pagination.
    """
    all_data = []
    offset = 0
    headers = {"X-App-Token": app_token} if app_token else {}

    # Add filter condition to fetch only rows where Locationabbr = 'US'
    where_condition = "Locationabbr = 'US'"

    while offset < max_records:
        print(f"Fetching records {offset} to {offset + batch_size} where {where_condition}...")
        
        # API query parameters
        params = {
            "$limit": batch_size,       # Max rows per request
            "$offset": offset,          # Pagination offset
            "$where": where_condition,  # Filtering logic
        }

        # Make the API request
        response = requests.get(api_url, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            if not data:  # Stop if no more data is returned
                print("No more data available.")
                break
            
            all_data.extend(data)
            offset += batch_size
            print(f"Fetched {len(data)} records (Total: {len(all_data)})")
        else:
            print(f"Error: {response.status_code} - {response.text}")
            break

    return all_data

# Main script logic
def main():
    # Step 1: Fetch data
    print("Starting data extraction...")
    data = fetch_all_data(API_URL, app_token=APP_TOKEN)

    if not data:
        print("No data fetched. Exiting.")
        return

    # Step 2: Convert to Pandas DataFrame
    print("Converting data to DataFrame...")
    df = pd.DataFrame(data)

    # Step 3: Save to a CSV file
    csv_filename = "brfss_data_us.csv"
    print(f"Saving data to {csv_filename}...")
    df.to_csv(csv_filename, index=False)
    print(f"Data saved to {csv_filename}")

# Run the script
if __name__ == "__main__":
    main()