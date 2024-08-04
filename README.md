# FGMC-Project
This project extract data from API and analysis using pyspark

# Design Decisions

1. Logging: 
   The Python logging module is used to record informational and error messages, aiding in process tracking and troubleshooting.

2. Schema Definition:
   Schemas are defined for nested JSON data to facilitate accurate Spark data interpretation.

3. Data Processing:
   Data from various URLs is consolidated into a JSON string, enabling unified processing of multiple data sources.
   
4. Data Transformation:
   Exploding Arrays: Nested array structures are flattened for easier analysis by exploding them into individual rows.
   Column Extraction and Renaming: Relevant columns are extracted and renamed to improve data clarity and accessibility.
   Postal Code Mapping: Postal codes are mapped to their corresponding provinces using a User-Defined Function (UDF) to    enrich the dataset with geographic information.
   One-Hot Encoding: Categorical data within the handoverServices array is transformed into numerical representations through one-hot encoding for machine learning compatibility.

5. Anonymization:
   Hashing is employed to anonymize sensitive data, safeguarding personal information from unauthorized access.

6. Data Partitioning:
   Data is stored in Parquet format, partitioned by province, to optimize query performance for provincial-level analysis.

Detailed Explanation for Comprehensive Analysis of Geographic Distances Using Apache Spark: API Data Extraction, Haversine Distance Calculation, and Metrics Enrichment"

1. Define the Haversine Function

Purpose: Accurately calculate the great-circle distance between two geographic points (latitude, longitude) on the Earth's spherical surface, essential for spatial analysis and proximity-based calculations.
Implementation: Employ the Haversine formula, a mathematical model that approximates the Earth as a sphere. The function takes latitude and longitude coordinates of two points as input and returns the distance in kilometers.

2. Create UDF for Haversine

Purpose: Integrate the Haversine function into Spark SQL operations for efficient distance calculations on large datasets.
Implementation: Define a User-Defined Function (UDF) named haversine_udf that encapsulates the Haversine function's logic. The UDF accepts four double-precision arguments (latitude1, longitude1, latitude2, longitude2) and returns a double-precision value representing the distance in kilometers.

3. Select Necessary Columns

Purpose: Optimize performance and memory usage by focusing on essential columns for distance calculations.
Implementation: Create a new DataFrame by selecting only the required columns: placeId, latitude, longitude, placeType_id, and commercialName from the original authorized_df. This reduced dataset streamlines subsequent computations.

4. Alias the DataFrames

Purpose: To distinguish between the two copies of the DataFrame when performing a self-join to calculate distances between all pairs of places.
Implementation: Create two aliases of the authorized_df: cust and other. This allows for a clear representation of the two datasets involved in the distance calculations

5. Calculate Distances

Purpose: Determine the great-circle distance between every combination of places in the dataset.
Implementation: Perform a cross-join between the aliased DataFrames cust and other to generate all possible pairs of places. Apply the haversine_udf to calculate the distance between each pair. Filter out rows where the placeId of cust and other are identical to avoid calculating distances to the same location.

6. Cache Distance DataFrame

Purpose: Enhance performance by storing the computed distances in memory for reuse, preventing redundant calculations.
Implementation: Apply the .cache() method to the distance_df DataFrame to instruct Spark to store the DataFrame in memory. This optimization is particularly effective when the distance DataFrame is accessed multiple times in subsequent operations.

7. Calculate Average Distance to Customers

Purpose: Determine the average distance of each place to all other places in the dataset.
Implementation: Group the distance_df by placeId, calculate the average distance using the avg function, and round the result to two decimal places.

8. Calculate Average Distance to Competitors

Purpose: Determine the average distance between places of the same type.
Implementation: Filter the distance_df to include only rows where the placeType_id of both places match. Group the filtered DataFrame by placeId and calculate the average distance, rounding to two decimal places.

9. Join Average Distances with Original DataFrame

Purpose: Incorporate the calculated average distances into the original DataFrame for further analysis and insights.
Implementation: Join the avg_distance_customers_df and avg_distance_competitors_df DataFrames back to the authorized_df based on the placeId column. Select the necessary columns, including placeId, commercialName, and the calculated average distances.

10. Fill Missing Values and Drop Duplicates

Purpose: Enhance data quality by handling missing values and removing duplicates.
Implementation: Replace missing values in the average_distance and average_distance_same_type columns with 0. Remove duplicate rows based on the placeId column to ensure data integrity.

11. Display Final Results

Purpose: Visualize the final DataFrame containing the enriched data.
Implementation: Display the cleaned_df to inspect the results of the data cleaning and enrichment process.
