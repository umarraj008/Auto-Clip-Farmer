import requests

# Your Twitch application credentials
CLIENT_ID = "tpi212qa424cievjqoblfdoqm3kapl"
CLIENT_SECRET = "7n80kkkb5qx4azzipv88dgsylrz5o4"

# The endpoint to request an OAuth token
TOKEN_URL = 'https://id.twitch.tv/oauth2/token'

# Step 1: Request OAuth token using client credentials flow
def get_oauth_token():
    # Data to send in the POST request to get the OAuth token
    data = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'grant_type': 'client_credentials'  # Use client credentials flow
    }
    
    # Make the POST request
    response = requests.post(TOKEN_URL, data=data)
    
    # Step 2: Handle the response
    if response.status_code == 200:
        # Parse the JSON response to get the token
        token_data = response.json()
        access_token = token_data['access_token']
        return access_token
    else:
        print(f"Error: Unable to get OAuth token. Status code: {response.status_code}")
        print(response.text)
        return None

# Get the OAuth token
bearer_token = get_oauth_token()

if bearer_token:
    print(f"Bearer Token: {bearer_token}")
else:
    print("Failed to retrieve Bearer Token.")
