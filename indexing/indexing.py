import os
import asyncio
import aiohttp
import json
from oauth2client.service_account import ServiceAccountCredentials
from aiohttp.client_exceptions import ServerDisconnectedError
from tqdm import tqdm

# Constants
SCOPES = ["https://www.googleapis.com/auth/indexing"]
ENDPOINT = "https://indexing.googleapis.com/v3/urlNotifications:publish"

async def send_url(session, http, url):
    content = {
        'url': url.strip(),
        'type': "URL_UPDATED"
    }
    for _ in range(3):  # Retry up to 3 times
        try:
            async with session.post(ENDPOINT, json=content, headers={"Authorization": f"Bearer {http}"}, ssl=False) as response:
                return await response.text()
        except ServerDisconnectedError:
            await asyncio.sleep(2)  # Wait for 2 seconds before retrying
            continue
    return '{"error": {"code": 500, "message": "Server Disconnected after multiple retries"}}'  # Return a custom error message after all retries fail

async def indexURL(http, urls):
    successful_urls = 0
    error_429_count = 0
    other_errors_count = 0
    tasks = []

    async with aiohttp.ClientSession() as session:
        # Using tqdm for progress bar
        for url in tqdm(urls, desc="Processing URLs", unit="url"):
            tasks.append(send_url(session, http, url))

        results = await asyncio.gather(*tasks)

        for result in results:
            data = json.loads(result)
            if "error" in data:
                if data["error"]["code"] == 429:
                    error_429_count += 1
                else:
                    other_errors_count += 1
            else:
                successful_urls += 1

    print(f"\nTotal URLs Tried: {len(urls)}")
    print(f"Successful URLs: {successful_urls}")
    print(f"URLs with Error 429: {error_429_count}")

def setup_http_client(json_key_file):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(json_key_file, scopes=SCOPES)
    token = credentials.get_access_token().access_token
    return token

def main():
    # Get parameters from environment variables
    try:
        num_accounts = int(os.getenv('NUM_ACCOUNTS', '1'))
    except ValueError:
        print("Invalid number of accounts. Please enter a valid number.")
        return
    
    url = os.getenv('URL')
    if not url:
        print("Error: URL parameter not provided!")
        return

    urls_per_account = int(os.getenv('URLS_PER_ACCOUNT', '200'))

    # Generate the list of URLs to be processed
    all_urls = [url] * (num_accounts * urls_per_account)

    # Process URLs for each account
    for i in range(num_accounts):
        print(f"\nProcessing URLs for Account {i+1}...")
        json_key_file = f"account{i+1}.json"

        # Check if account JSON file exists
        if not os.path.exists(json_key_file):
            print(f"Error: {json_key_file} not found!")
            continue

        start_index = i * urls_per_account
        end_index = start_index + urls_per_account
        urls_for_account = all_urls[start_index:end_index]

        http = setup_http_client(json_key_file)
        asyncio.run(indexURL(http, urls_for_account))

# Call the main function
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nScript paused. Press Enter to resume or Ctrl+C again to exit.")
        input()
        main()
