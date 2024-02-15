# YouTube Data Harvesting and Warehousing

## Overview
This project aims to collect, store, and analyze data from YouTube channels using the YouTube API. It retrieves various information such as channel details, video details, comments, and playlists, and stores them in both MongoDB and MySQL databases. The project also provides functionalities for querying and visualizing the data using Streamlit, allowing users to interact with the collected YouTube data easily.

## Technologies & Skills Used
- Python Scripting
- YouTube Data API Integration
- Data Management using MongoDB & MySQL
- Streamlit for User Interface

## Project Structure
The project consists of the following components:

1. **Data Collection**
    - Utilizes the YouTube Data API to retrieve information such as channel details, video IDs, video details, comments, and playlists.
    - Functions are defined to gather data and store it in MongoDB collections.

2. **Data Warehousing**
    - Stores the collected data in both MongoDB and MySQL databases.
    - Utilizes pymongo for MongoDB and mysql.connector for MySQL connections.

3. **Streamlit Application**
    - Provides a user interface for interacting with the collected data.
    - Allows users to trigger data collection, migration, and querying functionalities.
    - Displays various tables and visualizations for exploring the YouTube data.

4. **SQL Queries**
    - Includes SQL queries to answer specific questions about the collected data stored in MySQL tables.
    - Queries are used to extract insights such as top videos by views, comments, likes, etc.

## Setup Instructions
1. Clone the repository to your local machine.
2. Install the required Python libraries listed in `requirements.txt`.
3. Obtain API keys for the YouTube Data API.
4. Update the `Api_Connect()` function in `main.py` with your API key.
5. Ensure MongoDB is installed and running locally on default port 27017.
6. Set up a MySQL database named `youtube_data` with appropriate credentials.
7. Run the `main.py` script to start the Streamlit application.

## Usage
1. **Data Collection**
    - Enter a YouTube channel ID in the Streamlit interface and click the "collect data and store data in MongoDB" button to initiate data collection.
2. **Data Migration**
    - Click the "Migrate data from MongoDB to MySQL" button in the Streamlit interface to transfer data from MongoDB to MySQL.
3. **Data Exploration**
    - Use the provided functionalities in the Streamlit interface to view tables and execute SQL queries for analyzing the collected YouTube data.

## Contributors
- Dhananchezhiyan (https://github.com/chezhiyan6928/) - Aspiring data scientist
