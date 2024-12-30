from typing import Any, Dict, List, Optional
import os
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
import snowflake.connector
import requests
import pandas as pd
# No more load_dotenv since we’re not using .env
import json
import io
import matplotlib
import matplotlib.pyplot as plt
import time

matplotlib.use('Agg')

# ------------------------
# HARDCODE YOUR CREDENTIALS
# ------------------------
# Slack
SLACK_BOT_TOKEN = "xoxb-8...."
SLACK_APP_TOKEN = "xapp-1-A..."

# Snowflake
USER = "..."
PASSWORD = "..."
ACCOUNT = "..."

# Programmatic Access Token for Analyst REST
PAT = "eyJ..."

# Analyst endpoint & semantic model
ANALYST_ENDPOINT = "https://demo72.snowflakecomputing.com/api/v2/cortex/analyst/message"
STAGE = "SETUP"
FILE = "SKICAR_Semantic_Model.yaml"

# If you have a DB/Schema for your semantic model
DATABASE = "SKICAR"
SCHEMA = "SKICAR_SCHEMA"

ENABLE_CHARTS = False
DEBUG = False

# ------------------------
# Slack Bolt app
# ------------------------
app = App(token=SLACK_BOT_TOKEN)
messages = []

@app.message("hello")
def message_hello(message, say):
    say(f"Hey there <@{message['user']}>!")
    say(
        text="Let's BUILD",
        blocks=[
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": ":snowflake: Let's BUILD!",
                }
            },
        ]
    )

@app.event("message")
def handle_message_events(ack, body, say):
    ack()
    prompt = body["event"]["text"]
    process_analyst_message(prompt, say)

@app.command("/askcortex")
def ask_cortex(ack, body, say):
    ack()
    prompt = body["text"]
    process_analyst_message(prompt, say)

def process_analyst_message(prompt, say) -> Any:
    say_question(prompt, say)
    response = query_cortex_analyst(prompt)
    content = response["message"]["content"]
    display_analyst_content(content, say)

def say_question(prompt, say):
    say(
        text="Question:",
        blocks=[
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"Question: {prompt}",
                }
            },
        ]
    )
    say(
        text="Snowflake Cortex Analyst is generating a response",
        blocks=[
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "plain_text",
                    "text": "Snowflake Cortex Analyst is generating a response. Please wait...",
                }
            },
            {"type": "divider"},
        ]
    )

def query_cortex_analyst(prompt) -> Dict[str, Any]:
    """
    Calls the Cortex Analyst REST API using your Programmatic Access Token (PAT).
    """
    request_body = {
        "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}],
        "semantic_model_file": f"@{DATABASE}.{SCHEMA}.{STAGE}/{FILE}",
    }
    if DEBUG:
        print("Request Body:", request_body)

    headers = {
        "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {PAT}",
    }

    resp = requests.post(
        url=ANALYST_ENDPOINT,
        json=request_body,
        headers=headers,
    )

    request_id = resp.headers.get("X-Snowflake-Request-Id")

    if resp.status_code == 200:
        if DEBUG:
            print("Analyst Response:", resp.text)
        return {**resp.json(), "request_id": request_id}
    else:
        raise Exception(
            f"Failed request (id: {request_id}) with status {resp.status_code}: {resp.text}"
        )

def display_analyst_content(
    content: List[Dict[str, str]],
    say=None
) -> None:
    if DEBUG:
        print("Analyst Content:", content)
    for item in content:
        if item["type"] == "sql":
            # Show the generated SQL
            say(
                text="Generated SQL",
                blocks=[
                    {
                        "type": "rich_text",
                        "elements": [
                            {
                                "type": "rich_text_preformatted",
                                "elements": [
                                    {
                                        "type": "text",
                                        "text": f"{item['statement']}"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            )
            # Execute the SQL in Snowflake
            df = pd.read_sql(item["statement"], CONN)
            say(
                text="Answer:",
                blocks=[
                    {
                        "type": "rich_text",
                        "elements": [
                            {
                                "type": "rich_text_quote",
                                "elements": [
                                    {
                                        "type": "text",
                                        "text": "Answer:",
                                        "style": {"bold": True}
                                    }
                                ]
                            },
                            {
                                "type": "rich_text_preformatted",
                                "elements": [
                                    {
                                        "type": "text",
                                        "text": f"{df.to_string()}"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            )
            if ENABLE_CHARTS and len(df.columns) > 1:
                chart_img_url = plot_chart(df)
                if chart_img_url is not None:
                    say(
                        text="Chart",
                        blocks=[
                            {
                                "type": "image",
                                "title": {"type": "plain_text", "text": "Chart"},
                                "block_id": "image",
                                "slack_file": {"url": f"{chart_img_url}"},
                                "alt_text": "Chart"
                            }
                        ]
                    )
        elif item["type"] == "text":
            # Display text response
            say(
                text="Answer:",
                blocks=[
                    {
                        "type": "rich_text",
                        "elements": [
                            {
                                "type": "rich_text_quote",
                                "elements": [
                                    {
                                        "type": "text",
                                        "text": f"{item['text']}"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            )
        elif item["type"] == "suggestions":
            # Display suggestions
            suggestions = (
                "You may try these suggested questions: \n\n- "
                + "\n- ".join(item["suggestions"])
                + "\n\nNOTE: There's a 150 char limit on Slack messages so alter the questions accordingly."
            )
            say(
                text="Suggestions:",
                blocks=[
                    {
                        "type": "rich_text",
                        "elements": [
                            {
                                "type": "rich_text_preformatted",
                                "elements": [
                                    {
                                        "type": "text",
                                        "text": f"{suggestions}"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            )

def plot_chart(df):
    plt.figure(figsize=(10, 6), facecolor="#333333")

    # Pie chart with dynamic column names
    plt.pie(
        df[df.columns[1]],
        labels=df[df.columns[0]],
        autopct="%1.1f%%",
        startangle=90,
        colors=["#1f77b4", "#ff7f0e"],
        textprops={"color": "white", "fontsize": 16}
    )

    plt.axis("equal")
    plt.gca().set_facecolor("#333333")
    plt.tight_layout()

    file_path_jpg = "pie_chart.jpg"
    plt.savefig(file_path_jpg, format="jpg")
    file_size = os.path.getsize(file_path_jpg)

    file_upload_url_response = app.client.files_getUploadURLExternal(
        filename=file_path_jpg, length=file_size
    )
    if DEBUG:
        print("File Upload URL Response:", file_upload_url_response)
    file_upload_url = file_upload_url_response["upload_url"]
    file_id = file_upload_url_response["file_id"]
    with open(file_path_jpg, "rb") as f:
        response = requests.post(file_upload_url, files={"file": f})

    img_url = None
    if response.status_code != 200:
        print("File upload failed", response.text)
    else:
        response = app.client.files_completeUploadExternal(
            files=[{"id": file_id, "title": "chart"}]
        )
        if DEBUG:
            print("Complete Upload Response:", response)
        img_url = response["files"][0]["permalink"]
        time.sleep(2)
    return img_url

def init():
    """
    Connect to Snowflake using username/password,
    storing the connection in global 'CONN'.
    """
    conn = snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT
    )
    return conn

# Start the SocketModeHandler
if __name__ == "__main__":
    # Connect to Snowflake
    CONN = init()
    if not CONN.rest.token:
        print("Error: Failed to connect to Snowflake!")
        quit()

    print("⚡️ Bolt app is running!")
    SocketModeHandler(app, SLACK_APP_TOKEN).start()
