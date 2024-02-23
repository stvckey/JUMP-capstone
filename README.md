# [L.A Crime Data Analysis](https://stvckey.github.io/JUMP-capstone/) - View the live site

## Data Extraction
- Source: https://data.lacity.org/Public-Safety/Crime-Data-from-2020-to-Present/2nrs-mtv8/about_data
- You can export the data directly on the source or by an api endpoint
- Chosen dataset is updated weekly
- Note: LAPD is having issues posting crime data, so update time was changed to bi-weekly
- Documentation on data source, extraction logic, and justification
- Lambda function was used for extraction and a data sample is provided.
- The data we are using comes from the L.A. Crime Data Analysis. It features crime
data in Los Angeles and additional details relating to the crime (location, weapons, etc).
We plan to extract the data from the provided API: https://data.lacity.org/resource/2nrs-mtv8.json
transform this semi-structured live data, and load it into a data lake as the initial process
of creating a pipeline on AWS

- Extraction code and a data sample.

## Data Transformation
- Data cleaning included validating victim age, time format, and location strings
- Main criminal codes were kept, while the others were grouped together
- Data was split into 10 tables
- Record ID was added to keep track of Division of Records Number across tables
- Duplicates and Null Values were dropped
- ETL Job script was used in AWS Glue for splitting the tables in the datalake
- For readabilty and later analysis, the semi-structured data will be cleaned and transformed. This
process will be done in the scheduled Lambda function which will fetch the data from the API and
after cleaning for any dupliates, null, outliers, etc, it will be then transferred to the s3 bucket.
     # Remove unwanted spaces from location
        entry['location'] = re.sub(r'\s+', ' ', entry.get('location', ''))
        """I used a 're' library to replace consecutive spaces"""
        

## Data Loading
- Loading process and design can be found in the presentation

## Data Analysis
- See `analytics/capstone-analytics.ipynb`
