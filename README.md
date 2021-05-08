# BachelorThesis_HashtagBackend
Hashtag analyze

@author Yang Haoran


src/main/resources/c3p0-config.xml: The config of the connection pool can be modified here.

src/main/resources/db.properities: The connection to the database can be modified here.

src/main/resources/test.txt: This file is used to test the steaming processing as the source.(NOT USED)

src/main/resources/tweet.json: This is the example tweet format from the Twitter API.

src/main/resources/twitter.properties: The config of the authority of Twitter API can be set here.



src/main/java/Entity: This is folder stores the entity of Hashtag.


src/main/java/hashtag_analyze_main:This is the main folder of the hashtag processing.

|---src/main/java/hashtag_analyze_main/V2: This is the hashtag stream processing using TwitterV2 API.

|------src/main/java/hashtag_analyze_main/V2/ProcessingTwitterV2: You can run the application in this class.

[Backup] In case the TwitterV2 API is down or reach the monthly limitation, you can use this backup to process the hashtag with TwitterV1 API!

|---src/main/java/hashtag_analyze_main/V1:This is the hashtag stream processing using TwitterV1 API.

|------src/main/java/hashtag_analyze_main/V1/ProcessingTwitterV1: You can run the application in this class.(backup)


src/main/java/util: This is the folder to store the utils for the test. You can use these classed when reading data from the database.
