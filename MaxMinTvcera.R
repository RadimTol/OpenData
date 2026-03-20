# install.packages(c(
#   "jsonlite","dplyr","purrr","stringr","readr",
#   "lubridate","rvest","xml2","tibble"
# ))

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

# ------------------------------------------------------------
# Pomocné funkce
# ------------------------------------------------------------

parse_chmi_json_values <- function(url) {
  x <- jsonlite::fromJSON(url, simplifyVector = FALSE)

  vals <- x$data$data$values
  hdr  <- x$data$data$header

  if (is.null(vals) || is.null(hdr)) {
    stop("V JSON nebyla nalezena očekávaná struktura data$data: ", url)
  }

  col_names <- str_split(hdr, ",", simplify = TRUE) |>
    as.character() |>
    trimws()

  out <- as_tibble(do.call(rbind, vals), .name_repair = "minimal")
  names(out) <- col_names[seq_len(ncol(out))]
  out
}

# JSON soubory pouze v rootu adresáře
get_root_json_files <- function(index_url) {
  page <- read_html(index_url)

  hrefs <- page |>
    html_elements("a") |>
    html_attr("href")

  files <- hrefs[
    str_detect(hrefs, "\\.json$") &
    !str_detect(hrefs, "/")
  ]

  paste0(index_url, files)
}

# Najdi meta1 pro konkrétní datum YYYYMMDD
get_meta1_url_for_date <- function(meta_url, day_date) {
  target_file <- paste0("meta1-", format(day_date, "%Y%m%d"), ".json")
  paste0(meta_url, target_file)
}

# Robustní převod sloupce s datem/časem na Date
parse_record_date <- function(x) {
  x <- as.character(x)

  # zkusíme běžné varianty
  dt <- suppressWarnings(ymd_hms(x, tz = "UTC", quiet = TRUE))
  d1 <- as.Date(dt)

  # fallback pro čisté datum
  d2 <- suppressWarnings(ymd(x, quiet = TRUE))

  # fallback pro ISO datum s časem bez klasického formátu
  d3 <- suppressWarnings(as.Date(substr(x, 1, 10)))

  coalesce(d1, d2, d3)
}

prepare_station_metadata <- function(meta_url) {
  meta_raw <- parse_chmi_json_values(meta_url)

  needed <- c("GH_ID", "FULL_NAME", "ELEVATION")
  missing_cols <- setdiff(needed, names(meta_raw))
  if (length(missing_cols) > 0) {
    stop("V meta1 chybí očekávané sloupce: ", paste(missing_cols, collapse = ", "))
  }

  meta_raw |>
    transmute(
      STATION   = as.character(GH_ID),
      NAME      = as.character(FULL_NAME),
      ELEVATION = suppressWarnings(as.numeric(ELEVATION))
    ) |>
    distinct(STATION, .keep_all = TRUE)
}

read_daily_data <- function(file_urls) {
  purrr::map_dfr(file_urls, function(u) {
    message("Načítám: ", u)

    df <- parse_chmi_json_values(u)

    needed <- c("STATION", "ELEMENT", "VAL")
    missing_cols <- setdiff(needed, names(df))
    if (length(missing_cols) > 0) {
      stop("V datovém souboru chybí očekávané sloupce: ",
           paste(missing_cols, collapse = ", "),
           " | soubor: ", u)
    }

    # denní data mohou mít datum ve sloupci DT nebo DATE
    date_col <- intersect(c("DT", "DATE"), names(df))
    if (length(date_col) == 0) {
      stop("V datovém souboru není sloupec DT ani DATE: ", u)
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

get_top_bottom3 <- function(df_day, station_meta) {
  joined <- df_day |>
    left_join(station_meta, by = "STATION")

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

# ------------------------------------------------------------
# Hlavní běh
# ------------------------------------------------------------

# 1) všechny JSON soubory v rootu daily
data_files <- get_root_json_files(base_data_url)

if (length(data_files) == 0) {
  stop("Nenalezeny žádné JSON soubory v rootu: ", base_data_url)
}

# 2) načtení všech root daily souborů
raw_data <- read_daily_data(data_files)

if (nrow(raw_data) == 0) {
  stop("Žádná data pro ELEMENT = T, TMA, TMI")
}

# 3) poslední dostupný den v datech
last_day <- max(raw_data$RECORD_DATE, na.rm = TRUE)
message("Poslední dostupný den v datech: ", format(last_day, "%Y-%m-%d"))

# 4) nech jen poslední den
last_day_data <- raw_data |>
  filter(RECORD_DATE == last_day)

# 5) metadata pro tento den
meta_url <- get_meta1_url_for_date(base_meta_url, last_day)
station_meta <- prepare_station_metadata(meta_url)

# 6) TOP / BOTTOM 3
result <- get_top_bottom3(last_day_data, station_meta)

# 7) výstup
readr::write_excel_csv(result, "MaxMinT_daily_last_day.csv", na = "")

message("Hotovo: MaxMinT_daily_last_day.csv")
print(result)
