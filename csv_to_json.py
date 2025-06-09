import pandas as pd
import json
import asyncio
import aiohttp
from datetime import datetime
from tqdm import tqdm
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Google Maps API configuration
GOOGLE_MAPS_API_KEY = os.getenv("API_KEY")
GEOCODING_ENABLED = bool(GOOGLE_MAPS_API_KEY)

if not GEOCODING_ENABLED:
    print("Warning: Google Maps API key not found. Geocoding will be disabled.")


async def geocode_address_async(session, address):
    """
    Geocode a single address asynchronously using Google Maps API
    Args:
        session: aiohttp.ClientSession
        address (str): Address to geocode
    Returns:
        tuple: (latitude, longitude) or (None, None) if geocoding fails or is disabled
    """
    if not GEOCODING_ENABLED or not address or pd.isna(address):
        return None, None
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"address": address, "key": GOOGLE_MAPS_API_KEY, "region": "ar"}
    try:
        async with session.get(url, params=params, timeout=10) as resp:
            data = await resp.json()
            if data.get("status") == "OK" and data.get("results"):
                location = data["results"][0]["geometry"]["location"]
                return location["lat"], location["lng"]
            print(f"Error geocoding address '{address}': {data.get('status')}")
            return None, None
    except Exception as e:
        print(f"Error geocoding address '{address}': {str(e)}")
        return None, None


def parse_date(date_str):
    """
    Parse date string from DD/MM/YYYY HH:MM format to datetime object
    """
    try:
        return datetime.strptime(date_str, "%d/%m/%Y %H:%M")
    except Exception as e:
        print(f"Error parsing date {date_str}: {e}")
        return None


def format_date(dt):
    """
    Format datetime object to ISO 8601 format with timezone
    """
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ") if dt else None


def process_stations(df):
    """
    Process the stations data and group prices by station and product

    Args:
        df (DataFrame): Input DataFrame with price data

    Returns:
        dict: Processed stations data with prices
    """
    # Sort by date to get the most recent price first
    df["fecha_vigencia_dt"] = df["fecha_vigencia"].apply(parse_date)
    df = df.sort_values("fecha_vigencia_dt", ascending=False)

    # Dictionary to store stations data
    stations = {}

    # Group by station
    for station_id, station_group in tqdm(
        df.groupby("idempresa"), desc="Processing stations"
    ):
        # Get station info from first row (most recent data)
        first_row = station_group.iloc[0]

        # Create station entry if it doesn't exist
        if station_id not in stations:
            station = {
                "stationId": int(station_id),
                "stationName": first_row["empresa"],
                "address": first_row["direccion"],
                "town": first_row["localidad"],
                "province": first_row["provincia"],
                "flag": first_row["empresabandera"],
                "flagId": int(first_row["idempresabandera"]),
                "coordinates": {
                    "lat": (
                        float(first_row["latitud"])
                        if pd.notna(first_row["latitud"])
                        else None
                    ),
                    "lng": (
                        float(first_row["longitud"])
                        if pd.notna(first_row["longitud"])
                        else None
                    ),
                },
                "products": {},
            }
            stations[station_id] = station
        else:
            station = stations[station_id]

        # Process products
        for (product_id, product_name), product_group in station_group.groupby(
            ["idproducto", "producto"]
        ):
            product_key = f"{product_id}_{product_name}"

            if product_key not in station["products"]:
                station["products"][product_key] = {
                    "productId": int(product_id),
                    "productName": product_name,
                    "prices": [],
                }

            # Add prices
            for _, price_row in product_group.iterrows():
                price_entry = {
                    "price": (
                        float(price_row["precio"])
                        if pd.notna(price_row["precio"])
                        else None
                    ),
                    "date": format_date(price_row["fecha_vigencia_dt"]),
                    "hourType": price_row["tipohorario"],
                    "hourTypeId": int(price_row["idtipohorario"]),
                }
                station["products"][product_key]["prices"].append(price_entry)

    return stations

    # Save the updated CSV for future use
    df.to_csv("precios-historicos-updated.csv", index=False)
    print(
        "Updated CSV with geocoded coordinates saved as 'precios-historicos-updated.csv'"
    )


async def validate_and_geocode_stations(stations, concurrent_requests=5):
    """
    Validate and geocode stations with missing coordinates asynchronously
    Args:
        stations (dict): Dictionary of stations data
        concurrent_requests (int): Max concurrent requests to Google API
    Returns:
        dict: Updated stations with geocoded coordinates
    """
    if not GEOCODING_ENABLED:
        print("Geocoding is disabled. Using existing coordinates.")
        return stations

    stations_to_geocode = []
    for station_id, station in stations.items():
        coords = station["coordinates"]
        if (
            coords["lat"] is None
            or coords["lng"] is None
            or coords["lat"] == 0
            or coords["lng"] == 0
        ):
            stations_to_geocode.append((station_id, station))

    if not stations_to_geocode:
        print("All stations have valid coordinates.")
        return stations

    print(f"Found {len(stations_to_geocode)} stations with missing/invalid coordinates")

    semaphore = asyncio.Semaphore(concurrent_requests)

    async def geocode_and_update(station_id, station, session):
        address = (
            f"{station['address']}, {station['town']}, {station['province']}, Argentina"
        )
        async with semaphore:
            lat, lng = await geocode_address_async(session, address)
            if lat and lng:
                station["coordinates"]["lat"] = lat
                station["coordinates"]["lng"] = lng
                print(f"✅ {station_id}: {address} => {lat}, {lng}")
            else:
                print(f"❌ {station_id}: {address} => Failed")
            await asyncio.sleep(0.1)  # Respect API rate limit

    async with aiohttp.ClientSession() as session:
        tasks = [
            geocode_and_update(station_id, station, session)
            for station_id, station in stations_to_geocode
        ]
        for f in tqdm(
            asyncio.as_completed(tasks), total=len(tasks), desc="Geocoding stations"
        ):
            await f
    return stations


def format_output(stations):
    """
    Format the stations data to the final output format

    Args:
        stations (dict): Processed stations data

    Returns:
        list: Formatted stations data for JSON output
    """
    output = []

    for station_id, station in stations.items():
        # Convert products from dict to list
        products_list = list(station["products"].values())

        # Create station entry
        station_entry = {
            "stationId": station["stationId"],
            "stationName": station["stationName"],
            "address": station["address"],
            "town": station["town"],
            "province": station["province"],
            "flag": station["flag"],
            "flagId": station["flagId"],
            "geometry": {
                "type": "Point",
                "coordinates": [
                    station["coordinates"]["lng"] or 0.0,
                    station["coordinates"]["lat"] or 0.0,
                ],
            },
            "products": products_list,
        }

        output.append(station_entry)

    return output


def main():
    # Read the CSV file
    print("Reading CSV file...")
    try:
        df = pd.read_csv("precios-historicos.csv")
        print(f"Successfully read {len(df)} rows")
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    # Process stations and prices
    print("\nProcessing stations and prices...")
    stations = process_stations(df)
    print(f"Processed {len(stations)} stations")

    # Validate and geocode coordinates if needed (async)
    print("\nValidating and geocoding coordinates...")
    if GEOCODING_ENABLED:
        stations = asyncio.run(
            validate_and_geocode_stations(stations, concurrent_requests=5)
        )
    else:
        stations = stations

    # Format the output
    print("\nFormatting output...")
    output_data = format_output(stations)

    # Write to JSON file
    output_file = "stations_prices.json"
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        print(f"\n✅ Successfully saved data to {output_file}")
    except Exception as e:
        print(f"Error writing to {output_file}: {e}")


if __name__ == "__main__":
    main()
