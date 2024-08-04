# FGMC-Project
This project extract data from API and analysis using pyspark

### Design Decisions

1. Logging: 
   - You use Python’s `logging` module to log various levels of messages (info, error) which helps in tracking the progress and diagnosing issues.

2. Schema Definition:
   - You define schemas for nested JSON structures which ensures that Spark can correctly interpret the incoming data format.

3. Data Processing:
   - You fetch data from multiple URLs, aggregate it, and handle it as a JSON string. This approach allows you to work with data from multiple sources in a unified way.

4. Data Transformation:
   - Exploding Arrays: You explode arrays to normalize nested structures.
   - Column Extraction and Renaming: You select and rename columns to make the DataFrame schema more intuitive.
   - Postal Code Mapping: You create a UDF to map postal codes to provinces, which adds useful geographic context.
   - One-Hot Encoding: You handle categorical data in the `handoverServices` array by creating one-hot encoded columns.

5. Anonymization:
   - Sensitive data is anonymized using hashing, which is a good practice for protecting personal information.

6. Data Partitioning:
   - Data is saved as Parquet files and partitioned by province, optimizing for performance when querying by province.


Detailed Explanation for Comprehensive Analysis of Geographic Distances Using Apache Spark: API Data Extraction, Haversine Distance Calculation, and Metrics Enrichment"

1. Define the Haversine Function

Purpose: To calculate the great-circle distance between two points on the Earth’s surface using their latitude and longitude.
Implementation: The haversine function uses the Haversine formula to compute distances in kilometers.

2. Create UDF for Haversine

Purpose: To use the Haversine function in Spark SQL transformations.
Implementation: haversine_udf is defined as a UDF (User Defined Function) with DoubleType as the return type.

3. Select Necessary Columns

Purpose: To streamline the DataFrame by selecting only relevant columns for distance calculations.
Implementation: authorized_df is reduced to include only "placeId", "latitude", "longitude", "placeType_id", and "commercialName".

4. Alias the DataFrames

Purpose: To facilitate self-joins for distance calculations.
Implementation: authorized_df is aliased as cust and other to represent two sets of places in the cross join.

5. Calculate Distances

Purpose: To compute distances between every pair of places using the Haversine formula.
Implementation: A cross join between cust and other is performed, and the Haversine UDF is applied to calculate distances. The filter ensures that distances between the same place (cust.placeId equals other.placeId) are not calculated.

7. Cache Distance DataFrame

Purpose: To optimize performance by avoiding recomputation of the distance DataFrame.
Implementation: distance_df is cached using .cache().

8. Calculate Average Distance to Customers

Purpose: To find the average distance of each place to all other places.
Implementation: Aggregates distances grouped by placeId and calculates the average, rounding the result to two decimal places.

9. Calculate Average Distance to Competitors

Purpose: To find the average distance of each place to places of the same type.
Implementation: Filters distances where the place types are the same and then calculates the average distance, rounding to two decimal places.

10. Join Average Distances with Original DataFrame

Purpose: To enrich the original DataFrame with calculated average distances.
Implementation: Joins avg_distance_customers_df and avg_distance_competitors_df back to authorized_df. Selected columns include place IDs, commercial names, and average distances.

11. Fill Missing Values and Drop Duplicates

Purpose: To clean up the resulting DataFrame by handling missing values and ensuring uniqueness.
Implementation: Missing values for average distances are filled with 0, and duplicates are removed.

12. Display Final Results

Purpose: To show the final enriched DataFrame with average distances.
Implementation: The final_df is displayed to show the results.
