#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(jsonlite)
  library(dplyr)
  library(purrr)
  library(stringr)
  library(tidyr)
  library(readr)
  library(tibble)
})

base_url <- "https://opendata.chmi.cz/meteorology/climate/historical/data/10min/2025"
station_id <- "11406"
year_value <- 2025L
output_file <- "ozesource.csv"

f_bins <- c(-Inf, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, Inf)
f_labels <- c("0-1","1-2","2-3","3-4","4-5","5-6","6-7","7-8","8-9","9-10","10+")
ssv_bins <- c(-Inf, 100, 200, 300, 400, 500, 600, Inf)
ssv_labels <- c("0-100","100-200","200-300","300-400","400-500","500-600","600+")

download_month_file <- function(year_value, month_value, station_id, base_url) {
  ym <- sprintf("%04d%02d", as.integer(year_value), as.integer(month_value))
  file_name <- sprintf("10m-0-20000-0-%s-%s.json", station_id, ym)
  url <- sprintf("%s/%s", base_url, file_name)
  dest <- tempfile(fileext = ".json")

  ok <- tryCatch({
    utils::download.file(url, destfile = dest, mode = "wb", quiet = TRUE)
    TRUE
  }, error = function(e) {
    message(sprintf("Soubor %s se nepodarilo stahnout: %s", file_name, e$message))
    FALSE
  })

  if (!ok || !file.exists(dest) || file.info(dest)$size <= 0) {
    return(NULL)
  }

  dest
}

flatten_json_to_tibble <- function(x) {
  if (is.null(x)) {
    return(tibble())
  }

  if (is.data.frame(x)) {
    return(as_tibble(x))
  }

  if (is.list(x)) {
    nms <- names(x)

    if (!is.null(nms) && "data" %in% nms) {
      return(flatten_json_to_tibble(x[["data"]]))
    }

    if (!is.null(nms) && "result" %in% nms) {
      return(flatten_json_to_tibble(x[["result"]]))
    }

    if (!is.null(nms) && "features" %in% nms) {
      features <- x[["features"]]
      if (is.list(features) && length(features) > 0) {
        rows <- purrr::map(features, function(f) {
          props <- f[["properties"]]
          if (is.null(props)) props <- f
          flatten_json_to_tibble(props)
        })
        return(bind_rows(rows))
      }
    }

    if (length(x) == 0) {
      return(tibble())
    }

    if (all(purrr::map_lgl(x, ~ is.atomic(.x) && length(.x) <= 1))) {
      return(as_tibble(x))
    }

    if (all(purrr::map_lgl(x, ~ is.data.frame(.x) || is.list(.x)))) {
      rows <- purrr::map(x, flatten_json_to_tibble)
      return(bind_rows(rows))
    }
  }

  tibble()
}

read_month_data <- function(path) {
  raw <- jsonlite::fromJSON(path, flatten = TRUE)
  tbl <- flatten_json_to_tibble(raw)

  if (nrow(tbl) == 0) {
    stop(sprintf("V souboru %s nebyla nalezena zadna data.", basename(path)))
  }

  needed <- c("STATION", "DT", "ELEMENT", "VAL")
  missing <- setdiff(needed, names(tbl))
  if (length(missing) > 0) {
    stop(sprintf(
      "V souboru %s chybi sloupce: %s",
      basename(path),
      paste(missing, collapse = ", ")
    ))
  }

  tbl %>%
    transmute(
      STATION = as.character(.data$STATION),
      DT = as.character(.data$DT),
      ELEMENT = as.character(.data$ELEMENT),
      VAL = suppressWarnings(as.numeric(.data$VAL))
    )
}

parse_dt_fields <- function(dt_char) {
  x <- str_trim(as.character(dt_char))

  dt_parsed <- suppressWarnings(as.POSIXct(
    x,
    tz = "UTC",
    tryFormats = c(
      "%Y-%m-%dT%H:%M:%S",
      "%Y-%m-%d %H:%M:%S",
      "%Y-%m-%dT%H:%M",
      "%Y-%m-%d %H:%M",
      "%Y%m%d%H%M%S",
      "%Y%m%d%H%M"
    )
  ))

  tibble(
    YEAR = as.integer(format(dt_parsed, "%Y")),
    MONTH = as.integer(format(dt_parsed, "%m")),
    TIME = format(dt_parsed, "%H:%M")
  )
}

assign_interval <- function(value, element) {
  out <- rep(NA_character_, length(value))

  idx_f <- which(element == "F" & !is.na(value))
  if (length(idx_f) > 0) {
    out[idx_f] <- cut(
      value[idx_f],
      breaks = f_bins,
      labels = f_labels,
      right = FALSE,
      include.lowest = TRUE
    ) |> as.character()
  }

  idx_ssv <- which(element == "SSV10M" & !is.na(value))
  if (length(idx_ssv) > 0) {
    out[idx_ssv] <- cut(
      value[idx_ssv],
      breaks = ssv_bins,
      labels = ssv_labels,
      right = FALSE,
      include.lowest = TRUE
    ) |> as.character()
  }

  out
}

process_year <- function(year_value = 2025L, station_id = "11406", base_url = base_url) {
  month_tbls <- vector("list", 12)

  for (m in 1:12) {
    json_path <- download_month_file(year_value, m, station_id, base_url)
    if (is.null(json_path)) {
      next
    }

    message(sprintf("Zpracovavam %04d-%02d", year_value, m))
    month_tbls[[m]] <- read_month_data(json_path)
  }

  data_all <- bind_rows(month_tbls)

  if (nrow(data_all) == 0) {
    stop("Nepodarilo se nacist zadna data.")
  }

  dt_parts <- parse_dt_fields(data_all$DT)

  data_all <- bind_cols(data_all, dt_parts) %>%
    filter(
      STATION == station_id,
      YEAR == year_value,
      ELEMENT %in% c("F", "SSV10M")
    ) %>%
    mutate(
      INTERVAL = assign_interval(VAL, ELEMENT)
    ) %>%
    filter(!is.na(INTERVAL), !is.na(TIME), !is.na(MONTH))

  if (nrow(data_all) == 0) {
    stop("Po filtraci nezustala zadna data pro ELEMENT = F nebo SSV10M.")
  }

  out <- data_all %>%
    count(STATION, YEAR, MONTH, TIME, ELEMENT, INTERVAL, name = "COUNT") %>%
    tidyr::pivot_wider(
      names_from = INTERVAL,
      values_from = COUNT,
      values_fill = 0
    ) %>%
    arrange(STATION, YEAR, MONTH, TIME, ELEMENT)

  ordered_cols <- c(
    "STATION", "YEAR", "MONTH", "TIME", "ELEMENT",
    f_labels, ssv_labels
  )
  existing_cols <- intersect(ordered_cols, names(out))
  remaining_cols <- setdiff(names(out), existing_cols)

  out %>%
    select(all_of(existing_cols), all_of(remaining_cols))
}

result <- process_year(year_value = year_value, station_id = station_id, base_url = base_url)
readr::write_excel_csv(result, output_file)

message(sprintf("Hotovo. Vystup ulozen do %s", output_file))
