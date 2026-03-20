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

  col_names <- stringr::str_split(hdr, ",", simplify = TRUE) |>
    as.character() |>
    trimws()

  out <- tibble::as_tibble(do.call(rbind, vals), .name_repair = "minimal")
  names(out) <- col_names[seq_len(ncol(out))]
  out
}

# JSON soubory pouze v rootu adresáře
get_root_json_files <- function(index_url) {
  page <- rvest::read_html(index_url)

  hrefs <- page |>
    rvest::html_elements("a") |>
    rvest::html_attr("href")

  hrefs <- hrefs[!is.na(hrefs)]

  files <- hrefs[
    stringr::str_detect(hrefs, "\\.json$") &
    !stringr::str_detect(hrefs, "/")
  ]

  unique(paste0(index_url, files))
}

# Najde poslední soubor podle patternu meta1/meta2-YYYYMMDD.json
get_latest_file_by_pattern <- function(index_url, pattern) {
  page <- rvest::read_html(index_url)

  hrefs <- page |>
    rvest::html_elements("a") |>
    rvest::html_attr("href")

  hrefs <- hrefs[!is.na(hrefs)]
  files <- hrefs[stringr::str_detect(hrefs, pattern)]

  if (length(files) == 0) {
    stop("Nenalezen soubor odpovídající patternu: ", pattern, " v ", index_url)
  }

  files <- sort(unique(files))
  paste0(index_url, files[length(files)])
}

prepare_station_metadata <- function(meta1_url) {
  meta_raw <- parse_chmi_json_values(meta1_url)

  needed <- c("WSI", "GH_ID", "FULL_NAME", "ELEVATION")
  missing_cols <- setdiff(needed, names(meta_raw))
  if (length(missing_cols) > 0) {
    stop("V meta1 chybí očekávané sloupce: ", paste(missing_cols, collapse = ", "))
  }

  meta_raw |>
    dplyr::transmute(
      WSI       = as.character(WSI),
      STATION   = as.character(GH_ID),
      NAME      = as.character(FULL_NAME),
      ELEVATION = suppressWarnings(as.numeric(ELEVATION))
    ) |>
    dplyr::distinct(WSI, .keep_all = TRUE)
}

get_wsi_for_elements <- function(meta2_url, elements = c("T", "TMA", "TMI")) {
  meta2 <- parse_chmi_json_values(meta2_url)

  needed <- c("WSI", "EG_EL_ABBREVIATION")
  missing_cols <- setdiff(needed, names(meta2))
  if (length(missing_cols) > 0) {
    stop("V meta2 chybí očekávané sloupce: ", paste(missing_cols, collapse = ", "))
  }

  meta2 |>
    dplyr::transmute(
      WSI = as.character(WSI),
      EG_EL_ABBREVIATION = as.character(EG_EL_ABBREVIATION)
    ) |>
    dplyr::filter(EG_EL_ABBREVIATION %in% elements) |>
    dplyr::distinct(WSI)
}

filter_files_by_wsi <- function(file_urls, wsi_vector) {
  if (length(file_urls) == 0 || length(wsi_vector) == 0) {
    return(character(0))
  }

  file_names <- basename(file_urls)

  keep <- vapply(file_names, function(fn) {
    any(stringr::str_detect(fn, stringr::fixed(wsi_vector)))
  }, logical(1))

  file_urls[keep]
}

parse_record_date <- function(x) {
  x <- as.character(x)

  dt1 <- suppressWarnings(lubridate::ymd_hms(x, tz = "UTC", quiet = TRUE))
  d1  <- as.Date(dt1)

  d2  <- suppressWarnings(lubridate::ymd(x, quiet = TRUE))
  d3  <- suppressWarnings(as.Date(substr(x, 1, 10)))

  dplyr::coalesce(d1, d2, d3)
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

    date_col <- intersect(c("DT", "DATE"), names(df))
    if (length(date_col) == 0) {
      stop("V souboru není DT ani DATE: ", u)
    }
    date_col <- date_col[1]

    df |>
      dplyr::mutate(
        STATION = as.character(STATION),
        ELEMENT = as.character(ELEMENT),
        VAL = suppressWarnings(as.numeric(VAL)),
        RECORD_DATE = parse_record_date(.data[[date_col]])
      ) |>
      dplyr::filter(ELEMENT %in% c("T", "TMA", "TMI")) |>
      dplyr::filter(!is.na(RECORD_DATE), !is.na(VAL))
  })
}

get_top_bottom3_daily <- function(df_day, station_meta) {
  joined <- df_day |>
    dplyr::left_join(
      station_meta |> dplyr::select(STATION, NAME, ELEVATION),
      by = "STATION"
    )

  highest <- joined |>
    dplyr::group_by(ELEMENT) |>
    dplyr::arrange(dplyr::desc(VAL), STATION, .by_group = TRUE) |>
    dplyr::slice_head(n = 3) |>
    dplyr::mutate(EXTREME = "MAX", RANK = dplyr::row_number()) |>
    dplyr::ungroup()

  lowest <- joined |>
    dplyr::group_by(ELEMENT) |>
    dplyr::arrange(VAL, STATION, .by_group = TRUE) |>
    dplyr::slice_head(n = 3) |>
    dplyr::mutate(EXTREME = "MIN", RANK = dplyr::row_number()) |>
    dplyr::ungroup()

  dplyr::bind_rows(highest, lowest) |>
    dplyr::select(RECORD_DATE, ELEMENT, EXTREME, RANK, STATION, NAME, ELEVATION, VAL) |>
    dplyr::arrange(ELEMENT, EXTREME, RANK)
}

# ------------------------------------------------------------
# Hlavní běh
# ------------------------------------------------------------

data_files_all <- get_root_json_files(base_data_url)

if (length(data_files_all) == 0) {
  stop("Nenalezeny žádné JSON soubory v rootu ", base_data_url)
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
station_meta <- prepare_station_metadata(meta1_url)

last_day_data <- raw_data |>
  dplyr::filter(RECORD_DATE == last_day)

result <- get_top_bottom3_daily(last_day_data, station_meta)

readr::write_excel_csv(result, "MaxMinT_daily_last_day.csv", na = "")

message("Hotovo: MaxMinT_daily_last_day.csv")
print(result)
