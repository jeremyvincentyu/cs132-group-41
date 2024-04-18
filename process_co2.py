import pandas

def get_missing_gdp(world_bank_frame: pandas.DataFrame,country: str,year: int)->float:
    #Choose only rows of the relevant country
    only_relevant_country = world_bank_frame[world_bank_frame["Country Name"] == country]
    
    #Choose only the GDP row
    only_gdp_row = only_relevant_country[only_relevant_country["Series Name"] == "GDP (current US$)"]
    
    #Choose only the relevant year
    relevant_year = only_gdp_row[[f"{year} [YR{year}]"]]

    #print(f"Retrieved {relevant_year.iloc[0]}")

    return float(relevant_year.iloc[0,0]) 

owid_dataset: pandas.DataFrame = pandas.read_csv("owid-co2-data.csv")
world_bank_dataset: pandas.DataFrame = pandas.read_csv("world_bank.csv").replace({"Viet Nam":"Vietnam"})

#print(owid_dataset)

countries= world_bank_dataset["Country Name"].dropna().unique()
print(f"Unique Countries: {countries}")
all_country_frames: list[pandas.DataFrame] = []

#Filter country
for every_country in countries:
    country_frame = owid_dataset[owid_dataset["country"] == every_country]
    all_country_frames.append(country_frame)


#Re-merge into a single dataframe
selected_country_frame = pandas.concat(all_country_frames)
earliest_year = 1946
latest_year = 2022

#Filter by year
starting_earliest_year = selected_country_frame[selected_country_frame["year"]>=earliest_year]
ending_latest_year = starting_earliest_year[starting_earliest_year["year"]<=latest_year]

#Select Only Relevant Data
relevant_columns_only = ending_latest_year[["country","year","gdp","cement_co2","coal_co2","oil_co2","gas_co2"]].copy()
relevant_columns_only["fossil_fuel_co2"] = relevant_columns_only["coal_co2"] + relevant_columns_only["oil_co2"] + relevant_columns_only["gas_co2"]
relevant_columns_only = relevant_columns_only[["country","year","gdp","cement_co2","fossil_fuel_co2"]]


#Detect Rows with missing Data
na_detection = relevant_columns_only.isna().any(axis=1)
print("Rows with invalid cells:")
with_missing = relevant_columns_only[na_detection].copy()


refilled_rows = []

refilled_dataset = relevant_columns_only.copy()

#Fill in the Missing Data
for every_row in with_missing.itertuples():
    try:
        missing_gdp = get_missing_gdp(world_bank_dataset,every_row.country,int(every_row.year))
    except:
        print(f"Could not fill year {every_row.year} for {every_row.country} using the world bank dataset")
        missing_gdp = None
    row_index = every_row.Index
    refilled_dataset.at[row_index,"gdp"] = missing_gdp

#Print Current Selection
print("Currently Selected:")
print(refilled_dataset.dropna())
refilled_dataset.dropna().to_csv("Combined_DataSet.csv")
