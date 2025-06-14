""" Streamlit Interface: FrontEnd """

import streamlit as st
from src.mongo_conn import (
    MongoDBConnection,
    QueryMongoDB
)

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
            "Query Channel"
        ]
    )
    return add_channel_tab, query_channel_tab

def create_st_selectbox(unique_channels):
    """ Method to create st.selectbox """
    return st.selectbox("Select a YouTube Channel", unique_channels)

if __name__ == "__main__":
    # Get MongoDB collection
    client = MongoDBConnection.get_database("st-db")
    collection = client["st-youtube_analytics"]
    query = QueryMongoDB(collection=collection)

    all_documents = query.get_all_documents()
    unique_channels = query.get_distinct_channel_names(all_docs=all_documents)

    # st tab
    tabs = create_st_tabs()

    with tabs[0]:
        text = add_new_channel()
        st.write(f'Channel Name to Track: {text.split("@")[1]}')
    with tabs[1]:
        selected_channel = create_st_selectbox(unique_channels)
        channel_stats = query.get_documents_by_channel_name(
            all_docs=all_documents,
            channel_name=selected_channel
        )
        st.write(channel_stats)
