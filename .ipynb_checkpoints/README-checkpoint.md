# Durham Property Webscraper

## Code Purpose
The script `webscraper.py` script in this repository will scrape property info from the [Durham, NC county website](https://www.dconc.gov/government/departments-f-z/tax-administration/real-estate-appraisal/real-property-record-search). It does not scrape the owner information or addresses of each property.

## Dependencies
The script will dump all data in to a `PostgreSQL` database called 'durhamprop'. It also uses a table in that database to find the websites it needs to scrape info from. These will be made available later.

## Notes
This could be sped up through multithreading of multiprocessing, but I figured it would be nicer to the county server to not bombard it with 5-10 requests at a time.