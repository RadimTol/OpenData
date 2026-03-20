library(jsonlite)
library(dplyr)
library(purrr)
library(stringr)
library(readr)
library(lubridate)
library(rvest)
library(xml2)
library(tibble)

base_data_url <- "https://opendata.chmi.cz/meteorology/climate/recent/data/daily/"
base_meta_url <- "https://opendata.chmi.cz/meteorology/climate/recent/metadata/"

parse_record_date <- function(x) {
  x <- as.character(x)

  dt1 <- suppressWarnings(ymd_hms(x, tz = "UTC", quiet = TRUE))
  d1  <- as.Date(dt1)

  d2  <- suppressWarnings(ymd(x, quiet = TRUE))
  d3  <- suppressWarnings(as.Date(substr(x, 1, 10)))

  coalesce(d1, d2, d3)
}

read_daily_data <- function(file_urls) {
  purrr::map_dfr(file_urls, function(u) {
    message("Načítám: ", u)

    df <- parse_chmi_json_values(u)

    date_col <- intersect(c("DT", "DATE"), names(df))
    if (length(date_col) == 0) {
      stop("V souboru není DT ani DATE: ", u)
    }
    date_col <- date_col[1]

    df |>
      mutate(
        STATION = as.character(STATION),
        ELEMENT = as.character(ELEMENT),
        VAL = suppressWarnings(as.numeric(VAL)),
        RECORD_DATE = parse_record_date(.data[[date_col]])
      ) |>
      filter(ELEMENT %in% c("T", "TMA", "TMI")) |>
      filter(!is.na(RECORD_DATE), !is.na(VAL))
  })
}

get_top_bottom3_daily <- function(df_day, station_meta) {
  joined <- df_day |>
    left_join(station_meta, by = c("STATION"))

  highest <- joined |>
    group_by(ELEMENT) |>
    arrange(desc(VAL), STATION, .by_group = TRUE) |>
    slice_head(n = 3) |>
    mutate(EXTREME = "MAX", RANK = row_number()) |>
    ungroup()

  lowest <- joined |>
    group_by(ELEMENT) |>
    arrange(VAL, STATION, .by_group = TRUE) |>
    slice_head(n = 3) |>
    mutate(EXTREME = "MIN", RANK = row_number()) |>
    ungroup()

  bind_rows(highest, lowest) |>
    select(RECORD_DATE, ELEMENT, EXTREME, RANK, STATION, NAME, ELEVATION, VAL) |>
    arrange(ELEMENT, EXTREME, RANK)
}

data_files_all <- get_root_json_files(base_data_url)
if (length(data_files_all) == 0) {
  stop("Nenalezeny JSON soubory v rootu ", base_data_url)
}

meta2_url <- get_latest_file_by_pattern(base_meta_url, "^meta2-\\d{8}\\.json$")
wsi_tbl   <- get_wsi_for_elements(meta2_url, c("T", "TMA", "TMI"))

data_files <- filter_files_by_wsi(data_files_all, wsi_tbl$WSI)

if (length(data_files) == 0) {
  stop("Po filtrování přes meta2 nezbyly žádné daily soubory.")
}

raw_data <- read_daily_data(data_files)

if (nrow(raw_data) == 0) {
  stop("V načtených daily souborech nejsou žádná data T/TMA/TMI.")
}

last_day <- max(raw_data$RECORD_DATE, na.rm = TRUE)
message("Poslední dostupný den: ", format(last_day, "%Y-%m-%d"))

meta1_url <- paste0(base_meta_url, "meta1-", format(last_day, "%Y%m%d"), ".json")
station_meta <- prepare_station_metadata(meta1_url) |>
  select(STATION, NAME, ELEVATION)

last_day_data <- raw_data |>
  filter(RECORD_DATE == last_day)

result <- get_top_bottom3_daily(last_day_data, station_meta)

write_excel_csv(result, "MaxMinT_daily_last_day.csv", na = "")
print(result)
