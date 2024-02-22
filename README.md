# [L.A Crime Data Analysis](http://34.233.135.176/) - View the live site

## Data Extraction
- Source: https://data.lacity.org/Public-Safety/Crime-Data-from-2020-to-Present/2nrs-mtv8/about_data
- You can export the data directly on the source or by an api endpoint
- Chosen dataset is updated weekly
- Note: LAPD is having issues posting crime data, so update time was changed to bi-weekly
- Documentation on data source, extraction logic, and justification
- Lambda function was used for extraction and a data sample is provided.

## Data Transformation
- Data cleaning included validating victim age, time format, and location strings
- Main criminal codes were kept, while the others were grouped together
- Data was split into 10 tables
- Record ID was added to keep track of Division of Records Number across tables
- Duplicates and Null Values were dropped
- ETL Job script was used in AWS Glue for splitting the tables in the datalake

## Data Loading
- Loading process and design can be found in the presentation

## Data Analysis
- See `analytics/capstone-analytics.ipynb`
