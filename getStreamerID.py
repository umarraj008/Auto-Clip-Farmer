import requests
import argparse

# Your Twitch application credentials
CLIENT_ID = "tpi212qa424cievjqoblfdoqm3kapl"
BEARER_TOKEN = "p4l9pj0zqzdqkn9x2q8kk0evuuw7n5"

# Function to get User ID from Twitch username (handle)
def get_user_id(username):
    url = f'https://api.twitch.tv/helix/users?login={username}'
    headers = {
        'Client-ID': CLIENT_ID,
        'Authorization': f'Bearer {BEARER_TOKEN}',
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        user_data = response.json()
        if user_data['data']:
            return user_data['data'][0]['id']  # Return the User ID (broadcaster_id)
        else:
            print(f"Error: Streamer {username} not found")
            return None
    else:
        print(f"Error: Unable to fetch user data for {username}. Status code: {response.status_code}")
        return None

# CLI argument parser
def main():
    # Initialize the argument parser
    parser = argparse.ArgumentParser(description="Get Twitch User ID (broadcaster_id) for a streamer")
    
    # Add the argument for username
    parser.add_argument('username', type=str, help='Twitch username of the streamer')

    # Parse the arguments
    args = parser.parse_args()

    # Get the User ID for the provided username
    user_id = get_user_id(args.username)

    if user_id:
        print(f"User ID for {args.username}: {user_id}")
    else:
        print(f"Failed to retrieve User ID for {args.username}")

if __name__ == "__main__":
    main()
