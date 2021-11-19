**Immigration Data**

This data comes from the US National Tourism and Trade Office and consist of a list of files containing immigration data of individuals coming to USA in the year 2016. Each file contains information of a different month, thus leaving us with 12 files.

Columns|Descriptiopn
--- | --- 
cicid | Unique record ID
i94yr | 4 digit year
i94mon | Numeric month
i94cit | 3 digit code for immigrant country of birth
i94res | 3 digit code for immigrant country of residence
i94port | Port of admission
arrdate | Arrival Date in the USA
i94mode | Mode of transportation (1 = Air; 2 = Sea; 3 = Land; 9 = Not reported)
i94addr | USA State of arrival
depdate | Departure Date from the USA
i94bir | Age of Respondent in Years
i94visa | Visa codes collapsed into three categories
count | Field used for summary statistics
dtadfile | Character Date Field - Date added to I-94 Files
visapost | Department of State where where Visa was issued
occup | Occupation that will be performed in U.S
entdepa | Arrival Flag - admitted or paroled into the U.S.
entdepd | Departure Flag - Departed, lost I-94 or is deceased
entdepu | Update Flag - Either apprehended, overstayed, adjusted to perm residence
matflag | Match flag - Match of arrival and departure records
biryear | 4 digit year of birth
dtaddto | Character Date Field - Date to which admitted to U.S. (allowed to stay until)
gender | Non-immigrant sex
insnum | INS number
airline | Airline used to arrive in U.S.
admnum | Admission Number
fltno | Flight number of Airline used to arrive in U.S.
visatype | Class of admission legally admitting the non-immigrant to temporarily stay in U.S.

**World Temperature Data**

This dataset came from Kaggle. 

Columns|Descriptiopn
--- | --- 
dt | Date
AverageTemperature | Global average land temperature in celsius
AverageTemperatureUncertainty | 95% confidence interval around the average
City | Name of City
Country | Name of Country
Latitude | City Latitude
Longitude | City Longitude

**U.S. City Demographic Data**


Columns|Descriptiopn
--- | --- 
City | City Name
State | US State where city is located
Median Age | Median age of the population
Male Population | Count of male population
Female Population | Count of female population
Total Population | Count of total population
Number of Veterans | Count of total Veterans
Foreign born | Count of residents of the city that were not born in the city
Average Household Size | Average city household size
State Code | Code of the US state
Race | Respondent race
Count | Count of city's individual per race