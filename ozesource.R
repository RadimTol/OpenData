#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(jsonlite)
  library(dplyr)
  library(tidyr)
  library(readr)
  library(purrr)
  library(stringr)
  library(tibble)
})

base_url <- "https://opendata.chmi.cz/meteorology/climate/historical/data/10min/2025"
station_code_full <- "0-20000-0-11406"
station_code_out  <- "11406"
year_value <- 2025L
output_file <- "ozesource.csv"

# Neprekrývající se intervaly
f_labels <- c("0-<1","1-<2","2-<3","3-<4","4-<5","5-<6","6-<7","7-<8","8-<9","9-<10","10+")
f_breaks <- c(0,1,2,3,4,5,6,7,8,9,10,Inf)

ssv_labels <- c("0-99","100-199","200-299","300-399","400-499","500-599","600+")
ssv_breaks <- c(0,100,200,300,400,500,600,Inf)

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

download_month_json <- function(year_value, month_value, station_code_full, base_url) {
  ym <- sprintf("%04d%02d", as.integer(year_value), as.integer(month_value))
  file_name <- sprintf("10m-0-20000-0-11406-%s.json", ym)
  url <- sprintf("%s/%s", base_url, file_name)
  tmp <- tempfile(fileext = ".json")

  ok <- tryCatch({
    utils::download.file(url, tmp, mode = "wb", quiet = TRUE)
    TRUE
  }, error = function(e) {
    message(sprintf("Soubor %s se nepodarilo stahnout: %s", file_name, e$message))
    FALSE
  })

  if (!ok || !file.exists(tmp) || is.na(file.info(tmp)$size) || file.info(tmp)$size <= 0) {
    return(NULL)
  }

  raw_json <- jsonlite::fromJSON(tmp, simplifyVector = FALSE)
  extract_values_tbl(raw_json)
}

assign_interval <- function(val, element) {
  out <- rep(NA_character_, length(val))

  idx_f <- which(element == "F" & !is.na(val) & val >= 0)
  if (length(idx_f) > 0) {
    out[idx_f] <- as.character(cut(
      val[idx_f],
      breaks = f_breaks,
      labels = f_labels,
      right = FALSE,
      include.lowest = TRUE
    ))
  }

  idx_ssv <- which(element == "SSV10M" & !is.na(val) & val >= 0)
  if (length(idx_ssv) > 0) {
    out[idx_ssv] <- as.character(cut(
      val[idx_ssv],
      breaks = ssv_breaks,
      labels = ssv_labels,
      right = FALSE,
      include.lowest = TRUE
    ))
  }

  out
}

read_year_data <- function(year_value, base_url, station_code_full) {
  monthly <- vector("list", 12)

  for (m in 1:12) {
    message(sprintf("Zpracovavam %04d-%02d", year_value, m))
    monthly[[m]] <- tryCatch(
      download_month_json(year_value, m, station_code_full, base_url),
      error = function(e) {
        message(sprintf("Mesic %02d preskocen: %s", m, e$message))
        NULL
      }
    )
  }

  bind_rows(monthly)
}

prepare_output <- function(df, station_code_full, station_code_out, year_value) {
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
    filter(
      STATION == station_code_full,
      ELEMENT %in% c("F", "SSV10M")
    ) %>%
    mutate(
      DT_PARSED = as.POSIXct(DT, format = "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
      YEAR = suppressWarnings(as.integer(format(DT_PARSED, "%Y"))),
      MONTH = suppressWarnings(as.integer(format(DT_PARSED, "%m"))),
      TIME = ifelse(is.na(DT_PARSED), NA_character_, format(DT_PARSED, "%H:%M")),
      STATION = station_code_out,
      INTERVAL = assign_interval(VAL, ELEMENT)
    ) %>%
    filter(
      YEAR == year_value,
      !is.na(MONTH),
      !is.na(TIME),
      !is.na(INTERVAL)
    )

  if (nrow(df2) == 0) {
    stop("Po filtraci nezustala zadna data pro ELEMENT F nebo SSV10M.")
  }

  out <- df2 %>%
    group_by(STATION, YEAR, MONTH, TIME, ELEMENT, INTERVAL) %>%
    summarise(
      COUNT = n(),
      AVG = round(mean(VAL, na.rm = TRUE), 3),
      .groups = "drop"
    ) %>%
    arrange(STATION, YEAR, MONTH, TIME, ELEMENT, INTERVAL) %>%
    rename(BIN = INTERVAL)

  out
}

main <- function() {
  raw_df <- read_year_data(
    year_value = year_value,
    base_url = base_url,
    station_code_full = station_code_full
  )

  if (nrow(raw_df) == 0) {
    stop("Nepodarilo se nacist zadna data.")
  }

  result <- prepare_output(
    df = raw_df,
    station_code_full = station_code_full,
    station_code_out = station_code_out,
    year_value = year_value
  )

  readr::write_excel_csv(result, output_file)
  message(sprintf("Hotovo. Vystup ulozen do %s", output_file))
}

main()
