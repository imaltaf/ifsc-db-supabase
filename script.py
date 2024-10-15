import csv
import os
import requests
import asyncio
import uuid
from io import StringIO
from supabase import create_client, Client
from telegram.ext import Application

# Initialize Supabase client
supabase: Client = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))

# Initialize Telegram bot
telegram_bot = Application.builder().token(os.getenv('TELEGRAM_BOT_TOKEN')).build()

# Define the expected attributes
EXPECTED_ATTRIBUTES = [
    "BANK", "IFSC", "BRANCH", "CENTRE", "DISTRICT", "STATE", "ADDRESS", "CONTACT",
    "IMPS", "RTGS", "CITY", "ISO3166", "NEFT", "MICR", "UPI"
]

def download_csv(url):
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for bad status codes
    return StringIO(response.text)

async def send_telegram_message(message):
    chat_id = os.getenv('TELEGRAM_CHAT_ID')
    await telegram_bot.bot.send_message(chat_id=chat_id, text=message)

def convert_to_boolean(value):
    return str(value).lower() in ('true', 'yes', '1', 't', 'y')

def process_row(row):
    processed_row = {}
    for key, value in row.items():
        if key not in EXPECTED_ATTRIBUTES:
            continue  # Skip unexpected attributes
        if key in ['IMPS', 'RTGS', 'NEFT', 'UPI']:
            processed_row[key] = convert_to_boolean(value)
        elif key == 'ISO3166':
            processed_row[key] = str(value)[:2]  # Truncate to 2 characters
        else:
            processed_row[key] = str(value)  # Ensure all values are strings
    return processed_row

def import_csv_to_supabase(csv_file, table_name):
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        try:
            # Process the row to convert boolean fields, handle ISO3166, and filter attributes
            processed_row = process_row(row)
            
            # Insert the row into Supabase
            result = supabase.table(table_name).insert(processed_row).execute()
            print(f"Imported row: {processed_row['IFSC']}")
        except Exception as e:
            print(f"Error importing row: {row.get('IFSC', 'Unknown IFSC')}")
            print(f"Error details: {str(e)}")
            # You might want to log this error or handle it in some way
            continue  # Skip this row and continue with the next one

async def main():
    csv_url = os.getenv('CSV_URL')
    if not csv_url:
        raise ValueError("CSV_URL not found in environment variables")
    
    table_name = os.getenv('SUPABASE_TABLE_NAME')
    
    try:
        csv_file = download_csv(csv_url)
        import_csv_to_supabase(csv_file, table_name)
        await send_telegram_message("CSV import to Supabase completed successfully!")
    except requests.RequestException as e:
        error_message = f"Error occurred while downloading or processing CSV: {e}"
        print(error_message)
        await send_telegram_message(error_message)
    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        print(error_message)
        await send_telegram_message(error_message)

if __name__ == "__main__":
    asyncio.run(main())
