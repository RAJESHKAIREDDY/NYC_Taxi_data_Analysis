# Nyc Taxi Data Yellow cabs
NewYork data analysis on Yellow cabs in the year 2018.
Data retrieved from: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
I used Hadoop MapReduce,Hive and Pig for analyzing the trends in the Nyc Taxi dataset in the year 2018.

# PreProcessing
The dataset was initially stored in the Parquet format, which is a widely-used columnar storage file format specifically designed for efficient big data processing and analytics. I utilized a Python script to convert the Parquet format to CSV, which provides an easily readable dataset, enabling the identification of any anomalous data. After converting the format to CSV, I noticed that the file size was considerably large for each month in the year 2018. To manage this, I decided to extract 50,000 rows from each month's data, making it more manageable. By doing so, I was able to merge all the monthly files into a single CSV file. I made modifications to several columns using VLOOKUP in Excel. Specifically, I extracted specific details such as Borrow (street), Zone (Area), and Service Zone from the PickUp and DropOff locations. Additionally, I split the datetime column into three separate columns: Date, Time, and Day. These modifications allowed me to analyze and identify trends in the dataset more effectively. I utilized the modified merged CSV file for analysis using Hive, Pig, and Hadoop MapReduce.

# Key technologies used and their usage
1. **Hadoop MapReduce:** Min and Max Algorithm, Average Algorthim, Word count sort with decreasing order using Comparator in Reducer
2. **Hive:** Executed 11 HiveQL queries to analyze the emerging patterns in PickUp Locations.
3. **Pig:** Conducted thorough analysis on Drop Off Locations by executing four Pig scripts to identify emerging trends.


# Hive Analysis:(PickUpLocations)
1. how many records has each Tpep provider provided?
2. compare the overall average total_amount per trip for every month
3. no: of passengers per trip- how many trips are made by each level of 'passenger_count'
4. most preffered payment mode
5. most number of pickuplocations ID in every month
6. peakdates for pickup locations
7. peakdays for pickup locations
8. peakhours for overall  pickup locations
9. peakhours with respect to individual pickuplocations ID
10. find number of shortest distance & longest distance count and with respect to pickuplocations id
11. passenger popularity  with respect to short distance and long distance
12. peak hours for short trips/long trips


# Pig Analysis:(DropOff Locations)
1. average amount for every week
2. drivers with max/min  distance travelled.
3. most number of drop_off locations ID
4. peakdays for dropoff locations
5. peak hours for dropoff locations with dropff_area
6. shortest and longest distance with locationsID ,dropoff_street,dropoff_area
7. peak hours for shortest and longest distant locations.


# MapReduce Analysis:
1. Implemented a reducer with a comparator to sort the word count values in decreasing order.
2. Executed a MapReduce function to determine the minimum and maximum time based on the date.
3. Utilized a custom writable to perform a MapReduce function on calculating the average trip distance by month.
4. Identified the minimum and maximum distances for pickup locations.


# Results
   **PickUp Locations:-**

1. **VendorId-2** has more drives than VendorId-1.
2. **October and May** are the top two months for collecting the **average total amount** for drives in 2018.
3. **Single passenger trips** are more common than trips driven by multiple passengers.
4. **Credit Card payment** is the most popular payment option chosen by people.
5. **Manhattan (upper side south) and Queens (LaGuardia airport)** consistently remain in the top two positions for locations in almost every month.
6. **May, November, and December** have the busiest dates in terms of the number of trips in the holiday months.
7. **Wednesday and Tuesday, with Location ID 237**, are the **busiest weekdays**, while **Friday with Location ID 237 and Saturday with Location ID 79 are the busiest weekends**.
8. The **peak hours** for overall trips are from **6:00 PM to 7:30 PM**.
9. **Manhattan (Location ID 237, 236, 161)** is the top choice for **short trips**, while **Queens (Location ID 132, 138)** is preferred for **long trips**.
10. **JFK airport** is popular among passengers for **long-distance trips**, while **Upper East Side South and Times Sq/Theatre District** are popular for **short-distance trips**.
11. The **peak hours for short trips** are from **6:00 PM to 7:30 PM**, and the **peak hours for the longest trips** are from **7:00 AM to 9:00 AM**.


**DropOff Locations:-**

1. **Thursday and Friday** are the top days in terms of collecting more **amount** per week.
2. The **maximum distance for VendorID 2** is **213.2**, while for **VendorID 1**, it is **91.9**. The **minimum distance** for **both** VendorID 1 and VendorID 2 is **0.01**.
3. **(Manhattan, Upper East Side North) with Location ID 22755** has the **highest** number of **drop-off locations**.
4. **Thursday with the Manhattan-midtown Center** location holds the first place for **peak days** in the week.
5. The ***peak hours** for drop-off drives are from **6:45 PM to 7:30 PM**, while the **off-peak hours** are from **5:00 AM to 5:30 AM**.
6. **(Location ID 236, Manhattan, Upper East Side North) and (Location ID 161, Manhattan, Midtown Center)** are the two locations with the **shortest distance**, while **EWR-NEWARK Airport and Queens-JFK Airport** are the two locations with the **longest distance**.
7. **Manhattan, Midtown Center** has peak hours from **7:00 AM to 8:00 AM**, which are the peak hours for the shortest distance.
8. Unknown locations have peak hours around 7:00 PM to 8:00 PM, which are the peak hours for the longest distance.









