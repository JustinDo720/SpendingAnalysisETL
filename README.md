# Spending Analysis ETL

## Primary Function 

Microservice to **extract** data from our Django Restapi, **transform** our data (maybe include Vertex AI for more insightful data) and **load** into *Snowflake*.

## Development 

**07/18/25**
- Building an **extractor** to **ingest** data from our Django RestAPI
  - Two functions to Grab all the files + Find all the summary for it 
- **transformer** to **add/change** information based on our extractor 

**07/21/25** 
- Added more key fields for extra data:
  - Averaging Category & Vendor 
  - Percent Change over Time for Category & Vendor 
- Vertex AI New SDK to generate a summary based on our financial data 
  - IAM -> **Service Accounts** 
  - IAM Polcy ->
    - **Vertex AI User**
    - **Vertex AI Admin**
  - Json key 
    - Service Account --> Service Name --> Keys --> Generate JSON
  - Vertex AI API - Enable 