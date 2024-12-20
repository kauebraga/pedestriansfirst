library(sf)
library(mapview)
library(data.table)
library(dplyr)

# open new 2024 ghsl data
# this will be the default data for the next years?
# only needs to be run once
files <- dir("input_data/ghsl/ghsl_2024_raw/", full.names = TRUE, pattern = ".gpkg", recursive = TRUE)

# opne file
# variables <- files[1]
open_file <- function(variables) {
  
  region <- stringr::str_remove(basename(variables), "GHS_UCDB_REGION_")
  region <- stringr::str_remove(region, "_R2024A.gpkg")
  region <- tolower(region)
  region <- stringr::str_replace_all(region, "_", "-")
  
  file <- st_read(variables) %>%
    mutate(region = region)
  
}

ghsl_2024 <- lapply(files, open_file) %>% rbindlist() %>% st_sf() %>% 
  # st_transform(4326) %>%
  dplyr::mutate(ID_UC_G0 = as.integer(ID_UC_G0)) %>%
  dplyr::arrange(ID_UC_G0) %>%
# format the code to five characters
mutate(ID_UC_G0 = stringr::str_pad(ID_UC_G0, width = 5, side = "left", pad = 0))


# quickly format this file and export it back
ghsl_2024 %>%
  select(ID_UC_G0, NAME_MAIN = GC_UCN_MAI_2025, NAME_LIST = GC_UCN_LIS_2025, POP_2020 = GC_POP_TOT_2025, BU_m2_2020 = GC_UCA_KM2_2025) %>%
  st_write("input_data/ghsl/ghsl_2024.gpkg", append = FALSE)

ghsl_2024_filter <- ghsl_2024 %>%
  arrange(desc(GC_POP_TOT_2025)) %>%
  slice(1:1000) %>%
  arrange(ID_UC_G0)

# export to spreadsheet
ghsl_2024_filter1 <- ghsl_2024_filter %>%
  select(hdc = ID_UC_G0, region, country = GC_CNT_GAD_2025, name = GC_UCN_MAI_2025, name_long = GC_UCN_LIS_2025,  pop = GC_POP_TOT_2025, area = GC_UCA_KM2_2025) %>%
  # fix the region based on geofabrik - some of them will need to be changed manually
  mutate(region = case_when(
    region == "sub-saharan-africa" ~ "africa",
    region == "eastern-and-south-eastern-asia" ~ "asia",
    region == "latin-america-and-the-caribbean" ~ "south-america (CHECK)",
    region == "northern-america" ~ "north-america",
    region == "australia-and-new-zealand" ~ "australia-oceania",
    region == "central-and-southern-asia" ~ "asia",
    region == "northern-africa-and-western-asia" ~ "asia (CHECK)",
    .default = region
    
  )) %>%
  st_set_geometry(NULL) %>%
  # fix the check ones
  mutate(region = case_when(
    country == "Turkey" ~ "europe",
    country == "Russia" ~ "europe",
    country == "Georgia" ~ "europe",
    country %in% c("Costa Rica", "Cuba", "Dominican Republic", 
                   "El Salvador", "Guatemala", "Haiti", "Honduras", 
                   "Jamaica", "Nicaragua", "Panama", "Puerto Rico") ~ "central-america",
    country %in% c("México") ~ "north-america",
    country %in% c("Bahrain", "Iraq", "Israel", "Jordan", "Kuwait", "Lebanon", "Azerbaijan", "Armenia",
                   "Oman", "Palestine", "Qatar", "Saudi Arabia", "Syria", "United Arab Emirates", "Yemen") ~ "asia",
    region == "south-america (CHECK)" ~ "south-america",
    region == "asia (CHECK)" ~ "africa",
    .default = region
    
  )) %>%
  # inidivudllay fix some country names
  mutate(country1 = case_when(
    country == "Senegal" ~ "Senegal and Gambia",
    country == "Gambia" ~ "Senegal and Gambia",
    country == "Republic of the Congo" ~ "Congo Brazzaville",
    country == "Democratic Republic of the Congo" ~ "Congo Democratic Republic",
    country == "Côte d'Ivoire" ~ "Ivory Coast",
    country %in% c("Saudi Arabia", "Bahrain", "Qatar", "Kuwait", "United Arab Emirates", "Oman")  ~ "GCC States",
    country %in% c("Malaysia", "Singapore")  ~ "Malaysia Singapore Brunei",
    country %in% c("Israel", "Palestine")  ~ "Israel and Palestine",
    country == "United States" ~ "US",
    country == "Puerto Rico" ~ "US",
    country %in% c("Haiti", "Dominican Republic") ~ "Haiti and Domrep",
    country %in% c("Ireland") ~ "Ireland and Northern Ireland",
    country %in% c("Czechia") ~ "Czech Republic",
    .default = country
    
  )) %>%
  mutate(country1 = janitor::make_clean_names(country1, allow_dupes = TRUE)) %>%
  mutate(country1 = stringr::str_replace_all(country1, "_", "-"))


# save
fwrite(ghsl_2024_filter1, "input_data/pbf/pbf_hdc_country.csv")