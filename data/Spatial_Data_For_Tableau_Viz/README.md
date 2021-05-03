# Spatial Data Required Visualizing Realtor Data in Tableau on a Map
## Converting Zipcodes to Geographical Data
A dataset was required to get geographical coordinates for zipcodes. The dataset at https://public.opendatasoft.com/explore/dataset/us-zip-code-latitude-and-longitude/table/ 
was used to do this. Since we need .xlsx files for Tableau, but the website produces an .xls file, we opened the file in Microsoft Excel, and then exported the data to .xlsx to produce:

us-zip-code-latitude-and-longitude.xlsx

## US County Shapefile Data
We needed a shape file to be able to visualize the counties in the NYC Metro Area. We used https://www.census.gov/geographies/mapping-files/time-series/geo/carto-boundary-file.html to get this data, and the resulting files are contained in this directory. 
