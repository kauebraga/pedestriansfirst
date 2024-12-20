library(sf)
library(mapview)
library(data.table)
library(dplyr)
library(purrr)

# open ghsl table
ghsl_table <- fread("input_data/pbf/pbf_hdc_country.csv")

  
# create the file to open
# extract date
year <- format(Sys.Date(), "%Y")
month <- format(Sys.Date(), "%m")

# create the url to download countries from ghsl
ghsl_countries <- distinct(ghsl_table, region, country1) %>%
mutate(file_dir = sprintf("input_data/pbf/%s/%s_%s%s.osm.pbf", year, country1, year, month)) %>%
# create url
mutate(url = sprintf("https://download.geofabrik.de/%s/%s-latest.osm.pbf", region, country1)) %>%
arrange(region)


# # check if the urls work
# library(purrr)
# response <- lapply(ghsl_countries$url,
#   RCurl::url.exists)
# names(response) <- ghsl_countries$country
# response_error <- Filter(isFALSE, response)
  


# to download - south america to test
url_download <- ghsl_countries %>% filter(region == "south-america")
response_download <- purrr::map2(url_download$url,
                        url_download$file_dir,
                        possibly(curl::curl_download, quiet = TRUE, otherwise = "erro")
                      )
  

  
  