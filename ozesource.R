#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(jsonlite)
  library(dplyr)
  library(readr)
  library(purrr)
  library(stringr)
  library(tibble)
})

base_metadata_url <- "https://opendata.chmi.cz/meteorology/climate/historical/metadata/meta2.json"
base_data_url <- "https://opendata.chmi.cz/meteorology/climate/historical/data/10min/2025"
year_value <- 2025L
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

  required_cols <- c("WSI", "EG_EL_ABBREVIATION")
  missing_cols <- setdiff(required_cols, names(meta_tbl))
  if (length(missing_cols) > 0) {
    stop(sprintf("V meta2.json chybi sloupce: %s", paste(missing_cols, collapse = ", ")))
  }

  meta_tbl %>%
    transmute(
      WSI = as.character(WSI),
      EG_EL_ABBREVIATION = as.character(EG_EL_ABBREVIATION)
    ) %>%
    filter(EG_EL_ABBREVIATION %in% c("F", "SSV10M")) %>%
    distinct(WSI, EG_EL_ABBREVIATION) %>%
    group_by(WSI) %>%
    summarise(
      has_F = any(EG_EL_ABBREVIATION == "F"),
      has_SSV10M = any(EG_EL_ABBREVIATION == "SSV10M"),
      .groups = "drop"
    ) %>%
    filter(has_F, has_SSV10M) %>%
    pull(WSI) %>%
    sort()
}

download_station_month_json <- function(year_value, month_value, station_wsi, base_data_url) {
  ym <- sprintf("%04d%02d", as.integer(year_value), as.integer(month_value))
  file_name <- sprintf("10m-%s-%s.json", station_wsi, ym)
  url <- sprintf("%s/%s", base_data_url, file_name)

  tbl <- tryCatch(
    download_json_to_tbl(url),
    error = function(e) {
      message(sprintf("Stanice %s, mesic %02d preskocen: %s", station_wsi, month_value, e$message))
      NULL
    }
  )

  if (is.null(tbl)) {
    return(NULL)
  }

  tbl
}

read_year_data <- function(year_value, base_data_url, station_wsis) {
  all_tbls <- vector("list", length(station_wsis) * 12L)
  idx <- 1L

  for (station_wsi in station_wsis) {
    message(sprintf("Zpracovavam stanici %s", station_wsi))

    for (m in 1:12) {
      message(sprintf("  Mesic %04d-%02d", year_value, m))
      all_tbls[[idx]] <- download_station_month_json(
        year_value = year_value,
        month_value = m,
        station_wsi = station_wsi,
        base_data_url = base_data_url
      )
      idx <- idx + 1L
    }
  }

  bind_rows(all_tbls)
}

prepare_output <- function(df, year_value) {
  required_cols <- c("STATION", "ELEMENT", "DT", "VAL")
  missing_cols <- setdiff(required_cols, names(df))
  if (length(missing_cols) > 0) {
    stop(sprintf("Chybi sloupce: %s", paste(missing_cols, collapse = ", ")))
  }

  df2 <- df %>%
    transmute(
      STATION = as.character(STATION),
      ELEMENT = as.character(ELEMENT),
      DT = as.character(DT),
      VAL = suppressWarnings(as.numeric(VAL))
    ) %>%
    filter(ELEMENT %in% c("F", "SSV10M")) %>%
    mutate(
      DT_PARSED = as.POSIXct(DT, format = "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
      YEAR = suppressWarnings(as.integer(format(DT_PARSED, "%Y"))),
      MONTH = suppressWarnings(as.integer(format(DT_PARSED, "%m"))),
      TIME = ifelse(is.na(DT_PARSED), NA_character_, format(DT_PARSED, "%H:00"))
    ) %>%
    filter(
      YEAR == year_value,
      !is.na(MONTH),
      !is.na(TIME),
      !is.na(VAL)
    )

  if (nrow(df2) == 0) {
    stop("Po filtraci nezustala zadna data pro ELEMENT F nebo SSV10M.")
  }

  df2 %>%
    group_by(STATION, YEAR, MONTH, TIME, ELEMENT) %>%
    summarise(
      COUNT = n(),
      AVG = round(mean(VAL, na.rm = TRUE), 3),
      .groups = "drop"
    ) %>%
    arrange(STATION, YEAR, MONTH, TIME, ELEMENT)
}

main <- function() {
  station_wsis <- get_target_stations(base_metadata_url)

  if (length(station_wsis) == 0) {
    stop("V meta2.json nebyly nalezeny zadne stanice s prvky F a SSV10M.")
  }

  message(sprintf("Nalezeno %d stanic s prvky F a SSV10M.", length(station_wsis)))

  raw_df <- read_year_data(
    year_value = year_value,
    base_data_url = base_data_url,
    station_wsis = station_wsis
  )

  if (nrow(raw_df) == 0) {
    stop("Nepodarilo se nacist zadna data.")
  }

  result <- prepare_output(
    df = raw_df,
    year_value = year_value
  )

  readr::write_excel_csv(result, output_file)
  message(sprintf("Hotovo. Vystup ulozen do %s", output_file))
}

main()
