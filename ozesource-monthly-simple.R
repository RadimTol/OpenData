#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(jsonlite)
  library(dplyr)
  library(readr)
  library(tibble)
})

options(timeout = max(300, getOption("timeout")))

metadata_url <- "https://opendata.chmi.cz/meteorology/climate/historical/metadata/meta2.json"
monthly_base_url <- "https://opendata.chmi.cz/meteorology/climate/historical/data/monthly"
output_file <- "ozesource.csv"

extract_values_tbl <- function(raw_json) {
  vals <- raw_json$data$data$values
  hdr  <- raw_json$data$data$header

  if (is.null(vals) || length(vals) == 0) {
    stop("V JSON nebylo nalezeno pole data$data$values.")
  }

  header_names <- strsplit(hdr, ",", fixed = TRUE)[[1]]
  header_names <- trimws(header_names)

  tbl <- as_tibble(do.call(rbind, vals), .name_repair = "minimal")

  if (ncol(tbl) != length(header_names)) {
    stop(sprintf(
      "Pocet sloupcu v datech (%d) neodpovida hlavicce (%d).",
      ncol(tbl), length(header_names)
    ))
  }

  names(tbl) <- header_names
  tbl
}

download_json_to_tbl <- function(url) {
  tmp <- tempfile(fileext = ".json")

  ok <- tryCatch({
    utils::download.file(url, tmp, mode = "wb", quiet = TRUE)
    TRUE
  }, error = function(e) {
    message(sprintf("Soubor %s se nepodarilo stahnout: %s", url, e$message))
    FALSE
  })

  if (!ok || !file.exists(tmp) || is.na(file.info(tmp)$size) || file.info(tmp)$size <= 0) {
    return(NULL)
  }

  raw_json <- jsonlite::fromJSON(tmp, simplifyVector = FALSE)
  extract_values_tbl(raw_json)
}

get_target_stations <- function(metadata_url) {
  meta_tbl <- download_json_to_tbl(metadata_url)

  if (is.null(meta_tbl) || nrow(meta_tbl) == 0) {
    stop("Nepodarilo se nacist metadata meta2.json.")
  }

  required_cols <- c("OBS_TYPE", "WSI", "EG_EL_ABBREVIATION")
  missing_cols <- setdiff(required_cols, names(meta_tbl))
  if (length(missing_cols) > 0) {
    stop(sprintf("V meta2.json chybi sloupce: %s", paste(missing_cols, collapse = ", ")))
  }

  meta_tbl %>%
    transmute(
      OBS_TYPE = as.character(OBS_TYPE),
      WSI = as.character(WSI),
      EG_EL_ABBREVIATION = as.character(EG_EL_ABBREVIATION)
    ) %>%
    filter(
      OBS_TYPE == "DLY",
      EG_EL_ABBREVIATION %in% c("F", "SSV")
    ) %>%
    distinct(WSI, EG_EL_ABBREVIATION) %>%
    count(WSI, name = "N") %>%
    filter(N == 2) %>%
    pull(WSI) %>%
    sort()
}

download_station_monthly_json <- function(station_wsi, monthly_base_url) {
  file_name <- sprintf("mly-%s.json", station_wsi)
  url <- sprintf("%s/%s", monthly_base_url, file_name)

  tryCatch(
    download_json_to_tbl(url),
    error = function(e) {
      message(sprintf("Stanice %s preskocena: %s", station_wsi, e$message))
      NULL
    }
  )
}

read_all_station_data <- function(station_wsis, monthly_base_url) {
  station_tbls <- vector("list", length(station_wsis))

  for (i in seq_along(station_wsis)) {
    station_wsi <- station_wsis[[i]]
    message(sprintf("Zpracovavam stanici %s", station_wsi))
    station_tbls[[i]] <- download_station_monthly_json(
      station_wsi = station_wsi,
      monthly_base_url = monthly_base_url
    )
  }

  bind_rows(station_tbls)
}

prepare_output <- function(df) {
  required_cols <- c("STATION", "ELEMENT", "YEAR", "MONTH", "TIMEFUNCTION", "MDFUNCTION", "VALUE")
  missing_cols <- setdiff(required_cols, names(df))
  if (length(missing_cols) > 0) {
    stop(sprintf("Chybi sloupce: %s", paste(missing_cols, collapse = ", ")))
  }

  out <- df %>%
    transmute(
      STATION = as.character(STATION),
      ELEMENT = as.character(ELEMENT),
      YEAR = suppressWarnings(as.integer(YEAR)),
      MONTH = suppressWarnings(as.integer(MONTH)),
      TIMEFUNCTION = as.character(TIMEFUNCTION),
      MDFUNCTION = as.character(MDFUNCTION),
      AVG = suppressWarnings(as.numeric(VALUE))
    ) %>%
    filter(
      (ELEMENT == "F"   & TIMEFUNCTION == "AVG"   & MDFUNCTION == "AVG") |
      (ELEMENT == "SSV" & TIMEFUNCTION == "00:00" & MDFUNCTION == "SUM")
    ) %>%
    filter(
      !is.na(YEAR),
      !is.na(MONTH),
      !is.na(AVG)
    ) %>%
    select(STATION, YEAR, MONTH, ELEMENT, AVG) %>%
    distinct(STATION, YEAR, MONTH, ELEMENT, .keep_all = TRUE) %>%
    arrange(STATION, YEAR, MONTH, ELEMENT)

  if (nrow(out) == 0) {
    stop("Po filtraci nezustala zadna data pro F/AVG/AVG a SSV/00:00/SUM.")
  }

  out
}

main <- function() {
  station_wsis <- get_target_stations(metadata_url)

  if (length(station_wsis) == 0) {
    stop("V meta2.json nebyly nalezeny zadne stanice s OBS_TYPE=DLY a prvky F i SSV.")
  }

  message(sprintf("Nalezeno %d stanic s OBS_TYPE=DLY a prvky F i SSV.", length(station_wsis)))

  raw_df <- read_all_station_data(
    station_wsis = station_wsis,
    monthly_base_url = monthly_base_url
  )

  if (nrow(raw_df) == 0) {
    stop("Nepodarilo se nacist zadna mesicni data.")
  }

  result <- prepare_output(raw_df)

  readr::write_excel_csv(result, output_file)
  message(sprintf("Hotovo. Vystup ulozen do %s", output_file))
}

main()
