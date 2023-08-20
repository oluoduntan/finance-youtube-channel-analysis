# Finance Youtube Channel Analysis: Data Engineering Project

## Introduction 
In this project, I developed a batch ETL process to collect weekly data for five of my favorite YouTube channels: MeetKevin, Graham Stephan, Andrei Jikh, Tom Nash, and Financial Education. The ETL process collects data from the YouTube API, transforms the data, and stores it in a Google Sheet. The data is then visualized using Tableau.

The ETL process is executed on a weekly basis. It first retrieves the latest data for each channel from the YouTube API. The data includes the number of views, likes, and comments for each video. The data is then transformed to remove any duplicate or invalid data. The transformed data is then stored in a Google Sheet.

The data in the Google Sheet is then visualized using Tableau. Tableau is a data visualization software that allows users to create interactive dashboards and charts. I created a dashboard that displays the following metrics for each channel:

-   Number of views
-   Number of likes
-   Number of comments
-   Average view duration
-   Top 10 videos

The dashboard allows me to track the performance of each channel over time. I can also use the dashboard to identify trends and patterns in the data.

The ETL process and the Tableau dashboard have been helpful in tracking the performance of my favorite YouTube channels. The data has also been helpful in identifying trends and patterns in the YouTube ecosystem.

## Architecture 
<img src="Architecture.jpeg">

## Technology Used
- Programming Language - Python
- Data Storage - Google Sheets
- Data Visualization - Tableau
- Amazon Web Service (AWS)
	- Lambda
	- EventBridge
