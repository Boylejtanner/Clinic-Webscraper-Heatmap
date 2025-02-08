import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from geopy.geocoders import Nominatim

# Google Maps Geocoding API Key
API_KEY = "YOUR_API_KEY"

# Initialize geocoder
geolocator = Nominatim(user_agent="geoapi", timeout=10)

# CSV file path
CSV_FILE = "FILE_NAME_FOR_OUTPUT"

# Base URL for scraping YellowPages see line 156 to finish URL
BASE_URL = "https://www.yellowpages.ca/search/si"

# Headers for web requests
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
}

# Store default city coordinates dynamically
city_coordinates = {}

# Function to clean and standardize addresses
def clean_address(address):
    address = address.replace("Unknown", "").strip()
    address = ", ".join([part.strip() for part in address.split(",") if part.strip()])
    return address

# Extract city name from the address
def extract_city(address):
    try:
        parts = [part.strip() for part in address.split(",")]
        if len(parts) > 1:
            return parts[1]  # Return the second element as city
    except IndexError:
        pass
    return "Unknown"

# Get latitude and longitude using Google Geocoding API
def get_lat_lon_google(address):
    try:
        url = f"https://maps.googleapis.com/maps/api/geocode/json?address={address}&key={API_KEY}"
        response = requests.get(url)
        data = response.json()

        if data["status"] == "OK":
            location = data["results"][0]["geometry"]["location"]
            return location["lat"], location["lng"]
        else:
            print(f"Google Geocoding API failed for {address} with status {data['status']}")
    except Exception as e:
        print(f"Error querying Google Geocoding API for {address}: {e}")
    return None, None

# Get latitude and longitude with retry, using Google API first, then fallback to Nominatim
def get_lat_lon(address, retries=2):
    address = clean_address(address)
    city = extract_city(address)

    for attempt in range(retries):
        try:
            # Try Google Geocoding API
            lat, lon = get_lat_lon_google(address)
            if lat and lon:
                return lat, lon
        except:
            pass

        print(f"Retry {attempt + 1} failed for Google API: {address}")

        # Fallback to Nominatim on second attempt
        if attempt == 1:
            print(f"Using Nominatim fallback for: {address}")
            try:
                location = geolocator.geocode(address)
                if location:
                    return location.latitude, location.longitude
            except:
                pass

        time.sleep(1)

    print(f"Failed to get coordinates for: {address}")
    return None, None

# Enhanced function to classify clinic type
def classify_clinic_type(name):
    public_keywords = ["Hospital", "Health Centre", "Rehabilitation", "Institute", "Foundation"]
    private_keywords = ["Clinic", "Orthotics", "Prosthetics", "Center", "Foot", "Podiatry"]

    for keyword in public_keywords:
        if keyword.lower() in name.lower():
            return "Public"

    for keyword in private_keywords:
        if keyword.lower() in name.lower():
            return "Private"

    return "Private"

# Function to check for duplicates
def is_duplicate(existing_df, clinic_name, address, source=None):
    duplicate = (existing_df["Clinic Name"] == clinic_name) & (existing_df["Address"] == address)
    if source:
        duplicate = duplicate & (existing_df["Source"] == source)
    return duplicate.any()

# Load existing data
try:
    existing_df = pd.read_csv(CSV_FILE)
    print("Existing CSV found, loading data...")
except FileNotFoundError:
    print("No existing CSV found, starting fresh...")
    existing_df = pd.DataFrame(columns=["Clinic Name", "Address", "City", "Latitude", "Longitude", "Clinic Type", "Source"])

# Function to scrape a page
def scrape_page(url):
    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        print(f"Failed to retrieve {url}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
    clinics = soup.find_all("div", class_="listing__content__wrapper")

    new_data = []

    for clinic in clinics:
        try:
            name = clinic.find("a", class_="listing__name--link").text.strip()
            address = clinic.find("span", class_="listing__address--full").text.strip()
            address = clean_address(address)
            city = extract_city(address)

            # Check for duplicates
            if is_duplicate(existing_df, name, address):
                print(f"Skipping duplicate entry: {name}, {address}")
                continue

            # Get lat/lon
            latitude, longitude = get_lat_lon(address)

            # Classify clinic type
            clinic_type = classify_clinic_type(name)

            # Get clinic link
            link_tag = clinic.find("a", class_="listing__name--link")
            source_link = f"https://www.yellowpages.ca{link_tag['href']}" if link_tag else "Unknown"

            if is_duplicate(existing_df, name, address, source=source_link):
                print(f"Skipping duplicate with source: {name}, {address}, {source_link}")
                continue

            new_data.append({
                "Clinic Name": name,
                "Address": address,
                "City": city,
                "Latitude": latitude,
                "Longitude": longitude,
                "Clinic Type": clinic_type,
                "Source": source_link
            })

        except AttributeError:
            continue

    return new_data

# Scrape multiple pages
for page in range(1, 3):  # Adjust as needed
    print(f"Scraping page {page}...")
    page_url = f"{BASE_URL}/{page}/prosthetic/canada"  # Correct page URL, put the search query for YellowPages here
    new_data = scrape_page(page_url)
    if new_data:
        page_df = pd.DataFrame(new_data)

        # Append new data to the CSV incrementally
        try:
            existing_df = pd.read_csv(CSV_FILE)
            final_df = pd.concat([existing_df, page_df], ignore_index=True)
        except FileNotFoundError:
            final_df = page_df

        # Drop duplicates and save to CSV
        final_df.drop_duplicates(subset=["Clinic Name", "Address"], inplace=True)
        final_df.to_csv(CSV_FILE, index=False)
        print(f"Data from page {page} saved to {CSV_FILE}")
    time.sleep(2)

print("Scraping complete.")
