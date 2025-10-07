"""
CGWB API limits queries to 15-day chunks, so we split the date range accordingly.
Parallel groundwater data scraper for CGWB India WRIS portal.
Uses joblib to fetch data in parallel across multiple date ranges.
"""
##process can be optimised based on number of cores available

import requests
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
from joblib import Parallel, delayed
from time import sleep
import random

BASE_URL = "https://indiawris.gov.in/Dataset/Ground Water Level"


def fetch_groundwater_data(state, district, agency, start_date, end_date, download=True, page=0, size=1000):
    params = {
        "stateName": state,
        "districtName": district,
        "agencyName": agency,
        "startdate": start_date,
        "enddate": end_date,
        "download": str(download).lower(),
        "page": page,
        "size": size
    }
    headers = {"accept": "text/csv"}

    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.post(BASE_URL, params=params, headers=headers, timeout=30)
            if response.status_code == 200:
                return response.text
            else:
                print(f"Error {response.status_code} for {start_date} → {end_date}")
                if attempt < max_retries - 1:
                    sleep(2 ** attempt + random.uniform(0, 1))
        except requests.exceptions.RequestException as e:
            print(f"Request failed for {start_date} → {end_date}: {e}")
            if attempt < max_retries - 1:
                sleep(2 ** attempt + random.uniform(0, 1))

    return None


def split_date_range(start_date, end_date, step_days=15):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    ranges = []

    while start < end:
        next_date = min(start + timedelta(days=step_days - 1), end)
        ranges.append((start.strftime("%Y-%m-%d"), next_date.strftime("%Y-%m-%d")))
        start = next_date + timedelta(days=1)

    return ranges


def fetch_single_range(date_range, state, district, agency):
    s, e = date_range
    print(f"Fetching {s} → {e}")

    data = fetch_groundwater_data(state, district, agency, s, e)
    if data:
        try:
            df = pd.read_csv(StringIO(data))
            print(f"✓ {s} → {e}: {len(df)} records")
            return df
        except Exception as e:
            print(f"✗ Error parsing {s} → {e}: {e}")
            return None
    else:
        print(f"✗ No data for {s} → {e}")
        return None


def scrape_groundwater_parallel(state, district, agency, start_date, end_date,
                                step_days=15, n_jobs=4, outfile="groundwater_data.csv"):
    date_ranges = split_date_range(start_date, end_date, step_days)
    print(f" Total date ranges to fetch: {len(date_ranges)}")
    print(f" Using {n_jobs} parallel workers\n")

    results = Parallel(n_jobs=n_jobs, backend='threading')(
        delayed(fetch_single_range)(date_range, state, district, agency)
        for date_range in date_ranges
    )

    valid_dfs = [df for df in results if df is not None]

    if valid_dfs:
        final_df = pd.concat(valid_dfs, ignore_index=True)

        initial_count = len(final_df)
        final_df = final_df.drop_duplicates()

        final_df.to_csv(outfile, index=False)
        print(f"\n Data saved to {outfile}")
        print(f"   Total records: {len(final_df)}")
        if initial_count != len(final_df):
            print(f"   Removed {initial_count - len(final_df)} duplicates")
        return final_df
    else:
        print("\n❌ No data fetched.")
        return None


def scrape_groundwater(state, district, agency, start_date, end_date,
                       step_days=15, outfile="groundwater_data.csv"):
    all_dfs = []
    for s, e in split_date_range(start_date, end_date, step_days):
        print(f"Fetching {s} → {e}")
        data = fetch_groundwater_data(state, district, agency, s, e)
        if data:
            df = pd.read_csv(StringIO(data))
            all_dfs.append(df)

    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        final_df.to_csv(outfile, index=False)
        print(f" Data saved to {outfile} with {len(final_df)} records")
        return final_df
    else:
        print("No data fetched.")
        return None


if __name__ == "__main__":
    df = scrape_groundwater_parallel(
        state="Odisha",
        district="Baleshwar",
        agency="CGWB",
        start_date="2023-01-01",
        end_date="2025-09-30",
        step_days=15,
        n_jobs=4,
        outfile="baleshwar_groundwater_1.csv"
    )

    # df = scrape_groundwater(
    #     state="Odisha",
    #     district="Baleshwar", 
    #     agency="CGWB",
    #     start_date="2021-01-01",
    #     end_date="2025-09-30",
    #     step_days=15,
    #     outfile="baleshwar_groundwater.csv"
    # )