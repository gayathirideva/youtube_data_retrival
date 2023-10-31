# youtube_data_retrival


Application has :

-- Youtube API :
      Ability to input a Channel name and retrieve the relevent data using Google API

-- MongoDB :
      Here MongoDB works as a datalake. Ability to collect the data for all the 10 youtube channels and store them in data lake. And migrate the data from the data lake to the SQL Database as tables.

-- MySQL Workbench :
      Store the data from the data lake which is transformed and pushed to the desired table. Also to search and retrieve the data using search and joining tables to get the channel information.

-- StreamLit : 
      Display the data retrieved using streamlit app. Overall, giving a simple UI to make it more appealing to understand.

Project Brief :
This project aim is to extract data of a youtube channel using Youtube API. Later, take the required data based on the requirements. The data that is collected is pushe to the MongoDb for the storage, from where the data is transformed again and loaded to the MySQL Workbench. From the MySQL the data are accessed and loaded to the user defined queries where it is display the data based on the conditions. Here, for UI part streamlit has been implemented wherever it is required, for understanding the data and interactive purposes.

Configuration:

1.Open the project.py file in the project directory.

2.Set the desired configuration options:

3.Specify your YouTube API key.

4.Choose the database connection details (SQL and MongoDB).

5.Get the Youtube Channel ID from the Youtube's sourcepage

6.provide the Youtube Channel ID data to be harvested.

7.Set other configuration options as needed.
