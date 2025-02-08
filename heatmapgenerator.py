import pandas as pd
import folium
from folium.plugins import HeatMap

# CSV file path
CSV_FILE = "YOUR_CSV_FILE"

# Load the CSV file
try:
    df = pd.read_csv(CSV_FILE)
    print(f"Loaded {len(df)} rows from {CSV_FILE}.")
except FileNotFoundError:
    print(f"File not found: {CSV_FILE}")
    exit()

# Drop rows with missing latitude or longitude
df_cleaned = df.dropna(subset=["Latitude", "Longitude"])

# Ensure Latitude and Longitude columns are floats
df_cleaned["Latitude"] = pd.to_numeric(df_cleaned["Latitude"], errors="coerce")
df_cleaned["Longitude"] = pd.to_numeric(df_cleaned["Longitude"], errors="coerce")

# Drop rows where latitude or longitude is invalid
df_cleaned = df_cleaned.dropna(subset=["Latitude", "Longitude"])

# Get the mean latitude and longitude for the initial map center
mean_lat = df_cleaned["Latitude"].mean()
mean_lon = df_cleaned["Longitude"].mean()

# Create a base map centered on the mean latitude and longitude
heatmap_map = folium.Map(location=[mean_lat, mean_lon], zoom_start=5)

# Prepare data for HeatMap
heat_data = df_cleaned[["Latitude", "Longitude"]].values.tolist()

# Add heatmap layer to the map
HeatMap(heat_data).add_to(heatmap_map)

# Save the heatmap to an HTML file
OUTPUT_FILE = "YOUR_OUTPUT_FILE"
heatmap_map.save(OUTPUT_FILE)
print(f"Heatmap saved to {OUTPUT_FILE}.")