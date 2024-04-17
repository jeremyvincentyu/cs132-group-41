import pandas
owid_dataset: pandas.DataFrame = pandas.read_csv("owid-co2-data.csv")
world_bank_dataset: pandas.DataFrame = pandas.read_csv("world_bank.csv")


#print(owid_dataset)
#print(world_bank_dataset)

countries= ["Philippines"]
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
relevant_columns_only = ending_latest_year[["year","gdp","cement_co2","coal_co2","oil_co2","gas_co2"]].copy()
relevant_columns_only["fossil_fuel_co2"] = relevant_columns_only["coal_co2"] + relevant_columns_only["oil_co2"] + relevant_columns_only["gas_co2"]
relevant_columns_only = relevant_columns_only[["year","gdp","cement_co2","fossil_fuel_co2"]]


#Detect Rows with missing Data
na_detection = relevant_columns_only.isna()
#for every_index,every_row in na_detection.iterrows():
#    print(every_row)

#Fill in the Missing Data


#Print Current Selection
print("Currently Selected:")
print(relevant_columns_only)

