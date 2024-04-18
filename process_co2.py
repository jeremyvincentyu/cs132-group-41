import pandas

def get_missing_gdp(world_bank_frame: pandas.DataFrame,owid_frame: pandas.DataFrame,country: str,year: int)->float:
    #Choose only rows of the relevant country
    only_relevant_country = world_bank_frame[world_bank_frame["Country Name"] == country]
    
    #Choose only the GDP row
    only_gdp_row = only_relevant_country[only_relevant_country["Series Name"] == "GDP (current US$)"]
    
    #Choose only the relevant year
    relevant_year = only_gdp_row[[f"{year} [YR{year}]"]]

    #print(f"Retrieved {relevant_year.iloc[0]}")
    in_current_dollars = float(relevant_year.iloc[0,0])

    conversion_factor = 0
    #Find the closest previous year to the current year that both OWID and World bank reported a GDP
    for every_year in range(year-1,0,-1):
        print(f"Trying to see if conversion factor is obtainable from year {every_year} for year {year}")
        
        #Check OWID GDP
        country_frame = owid_frame[owid_frame["country"] == country]
        year_frame = country_frame[country_frame["year"] == every_year]
        owid_undefined = year_frame[["gdp"]].isna().iloc[0,0]

        #Assume first that the world bank has the year defined
        wb_undefined = False
        #Check World Bank GDP
        try:
            only_gdp_row[[f"{every_year} [YR{every_year}]"]]
        except:
            #If access fails, then the world bank is undefined for that year
            wb_undefined =  True
        print(f"For the year {every_year}, owid's na is {owid_undefined}, and wb's na is {wb_undefined}")
        #If neither is null, compute the conversion factor from that year and break
        if not owid_undefined and not wb_undefined:
            print("Starting conversion factor computation")
            owid_gdp = float(year_frame[["gdp"]].iloc[0,0])
            wb_gdp = float(only_gdp_row[[f"{every_year} [YR{every_year}]"]].iloc[0,0])

            #Compute the conversion factor by dividing the owid gdp by the world bank gdp of that year
            conversion_factor = owid_gdp/wb_gdp
            print(f"Using a conversion factor of {conversion_factor} for the year {year} from the year {every_year}")
            break

    in_2011_dollars = in_current_dollars*conversion_factor
    return  in_2011_dollars

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
        missing_gdp = get_missing_gdp(world_bank_dataset,owid_dataset,every_row.country,int(every_row.year))
    except:
        print(f"Could not fill year {every_row.year} for {every_row.country} using the world bank dataset")
        missing_gdp = None
    row_index = every_row.Index
    refilled_dataset.at[row_index,"gdp"] = missing_gdp

#Print Current Selection
print("Currently Selected:")
print(refilled_dataset.dropna())
refilled_dataset.dropna().to_csv("Combined_DataSet.csv")
