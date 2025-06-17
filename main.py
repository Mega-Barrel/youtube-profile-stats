""" Streamlit Interface: FrontEnd """
from src.mongo_conn import (
    MongoDBConnection,
    BaseMongoStore,
    YouTubeUserStore
)
from src.youtube_connection import YouTubeAPIConnection

import streamlit as st
import pandas as pd

def add_new_channel():
    """ Method to add new channels to track"""
    return st.text_input(
        "Enter YouTube Channel Link Below:",
        placeholder="https://www.youtube.com/@AnuragSalgaonkar",
    )

def create_st_tabs():
    """ Method to st.tabs() """
    add_channel_tab, query_channel_tab = st.tabs(
        [
            "Add Channel",
            "Channel Statistics"
        ]
    )
    return add_channel_tab, query_channel_tab

def create_st_selectbox(unique_channels):
    """ Method to create st.selectbox """
    return st.selectbox("Select a YouTube Channel", unique_channels)

# Add arrow symbols
def format_with_arrows(val):
    """ Add arrow annotators"""
    if val > 0:
        return f"⬆️ {val:.2f}%"
    elif val < 0:
        return f"⬇️ {abs(val):.2f}%"
    else:
        return "➖ 0.00%"

if __name__ == "__main__":
    # Initialize MongoDB
    db = MongoDBConnection.get_database("st-db")
    stats_collection = BaseMongoStore(db["st-youtube_analytics"])
    users_collection = db["youtube_users"]
    user_store = YouTubeUserStore(users_collection)

    # Initialize YouYubeAPIConnection
    yt = YouTubeAPIConnection()

    all_channels = [entry["channel_name"] for entry in user_store.get_all_documents()]

    # st tab
    tabs = create_st_tabs()

    with tabs[0]:
        text = add_new_channel()
        if text:
            channel_name = text.split("@")[1]
            st.write(f'Channel Name to Track: {text.split("@")[1]}')
            try:
                response = (
                    yt.channels()
                    .list(
                        part="id,snippet",
                        forHandle=channel_name
                    )
                    .execute()
                )
                resp = response.get("items", [])
                st.write(resp)
                if resp:
                    snippet = resp[0].get("snippet", {})
                    channel_id = resp[0].get("id")

                    if not channel_id or not snippet:
                        st.write("Missing required fields: id or snippet.")

                    channel_name = snippet.get("title", "Unknown Channel")

                    # Insert user if not already present
                    existing_user = users_collection.find_one({
                        "channel_id": channel_id,
                        "channel_name": channel_name
                    })

                    if not existing_user:
                        user_store.add_user(
                            channel_id=channel_id,
                            channel_name=channel_name
                        )
                        st.write(f"New Channel: {channel_name} added to tracking")
                    else:
                        st.write(f"Channel: {channel_name} already exists. Head over Channel Stats for detailed information")
            except Exception as e:
                st.write(f"Failed to fetch data for {channel_name}: {e}")

    with tabs[1]:
        selected_channel = create_st_selectbox(all_channels)
        channel_stats = stats_collection.get_all_documents(channel_name=selected_channel)
        if len(channel_stats) == 0:
            st.write('No data found..')
        else:
            df = pd.DataFrame(channel_stats)
            # Ensure correct type and sort
            df = df.sort_values('fetched_date')

            # Select relevant columns
            filtered_df = df[['fetched_date', 'video_count', 'subscriber_count', 'view_count']].copy()

            # Calculate daily percent change
            filtered_df['subscriber_change_%'] = filtered_df['subscriber_count'].pct_change().fillna(0) * 100
            filtered_df['view_change_%'] = filtered_df['view_count'].pct_change().fillna(0) * 100

            # Round for cleaner UI
            filtered_df['subscriber_change_%'] = filtered_df['subscriber_change_%'].round(2)
            filtered_df['view_change_%'] = filtered_df['view_change_%'].round(2)

            filtered_df['subscriber_change_%'] = filtered_df['subscriber_change_%'].apply(format_with_arrows)
            filtered_df['view_change_%'] = filtered_df['view_change_%'].apply(format_with_arrows)

            # Reset index for cleaner Streamlit display
            filtered_df.reset_index(drop=True, inplace=True)

            # Display
            st.write("📊 Channel Stats with Daily Change:")
            st.dataframe(filtered_df)
