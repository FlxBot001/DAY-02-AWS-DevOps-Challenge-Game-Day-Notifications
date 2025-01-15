import os
import json
import urllib.request
import boto3
from datetime import datetime, timedelta, timezone

# Format game data based on its status (Final, InProgress, Scheduled)
def format_game_data(game):
    status = game.get("Status", "Unknown")
    away_team = game.get("AwayTeam", "Unknown")
    home_team = game.get("HomeTeam", "Unknown")
    final_score = f"{game.get('AwayTeamScore', 'N/A')}-{game.get('HomeTeamScore', 'N/A')}"
    start_time = game.get("DateTime", "Unknown")
    channel = game.get("Channel", "Unknown")

    # Format quarter scores
    quarters = game.get("Quarters", [])
    quarter_scores = ', '.join([
        f"Q{q['Number']}: {q.get('AwayScore', 'N/A')}-{q.get('HomeScore', 'N/A')}" 
        for q in quarters
    ])

    # Return formatted message based on game status
    if status == "Final":
        return (
            f"Game Status: {status}\n"
            f"{away_team} vs {home_team}\n"
            f"Final Score: {final_score}\n"
            f"Start Time: {start_time}\n"
            f"Channel: {channel}\n"
            f"Quarter Scores: {quarter_scores}\n"
        )
    elif status == "InProgress":
        last_play = game.get("LastPlay", "N/A")
        return (
            f"Game Status: {status}\n"
            f"{away_team} vs {home_team}\n"
            f"Current Score: {final_score}\n"
            f"Last Play: {last_play}\n"
            f"Channel: {channel}\n"
        )
    elif status == "Scheduled":
        return (
            f"Game Status: {status}\n"
            f"{away_team} vs {home_team}\n"
            f"Start Time: {start_time}\n"
            f"Channel: {channel}\n"
        )
    else:
        return (
            f"Game Status: {status}\n"
            f"{away_team} vs {home_team}\n"
            f"Details are unavailable at the moment.\n"
        )

# Lambda function handler
def lambda_handler(event, context):
    # Get required environment variables
    api_key = os.getenv("NBA_API_KEY")
    sns_topic_arn = os.getenv("SNS_TOPIC_ARN")

    # Validate that critical environment variables are available
    if not api_key or not sns_topic_arn:
        print("Error: Missing required environment variables.")
        return {"statusCode": 500, "body": "Missing environment variables"}

    # Initialize the SNS client
    sns_client = boto3.client("sns")

    # Adjust time to Central Time (UTC-6)
    utc_now = datetime.now(timezone.utc)
    central_time = utc_now - timedelta(hours=6)
    today_date = central_time.strftime("%Y-%m-%d")

    print(f"Fetching games for date: {today_date}")

    # Construct the API URL
    api_url = f"https://api.sportsdata.io/v3/nba/scores/json/GamesByDate/{today_date}?key={api_key}"
    
    # Fetch game data from the API
    try:
        with urllib.request.urlopen(api_url) as response:
            data = json.loads(response.read().decode())
            print("Successfully fetched game data.")
            print(json.dumps(data, indent=4))  # Log the fetched data for debugging
    except urllib.error.HTTPError as e:
        print(f"HTTP Error: {e.code} - {e.reason}")
        return {"statusCode": e.code, "body": f"HTTP Error: {e.reason}"}
    except urllib.error.URLError as e:
        print(f"URL Error: {e.reason}")
        return {"statusCode": 500, "body": f"URL Error: {e.reason}"}
    except json.JSONDecodeError:
        print("Error decoding JSON response.")
        return {"statusCode": 500, "body": "Error decoding JSON response"}
    except Exception as e:
        print(f"Unexpected error: {e}")
        return {"statusCode": 500, "body": f"Unexpected error: {e}"}

    # Check if any games were returned
    if not data:
        print("No games available for today.")
        return {"statusCode": 200, "body": "No games available for today."}

    # Format the game data into messages
    messages = [format_game_data(game) for game in data]
    final_message = "\n---\n".join(messages)

    # Publish the formatted message to SNS
    try:
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Message=final_message,
            Subject="NBA Game Updates"
        )
        print("Message published to SNS successfully.")
    except sns_client.exceptions.ClientError as e:
        print(f"Error publishing to SNS: {e.response['Error']['Message']}")
        return {"statusCode": 500, "body": "Error publishing to SNS"}
    except Exception as e:
        print(f"Unexpected error while publishing to SNS: {e}")
        return {"statusCode": 500, "body": f"Unexpected error: {e}"}

    return {"statusCode": 200, "body": "Data processed and sent to SNS"}
