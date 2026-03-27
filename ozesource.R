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

f_bins <- c(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, Inf)
f_labels <- c("0-1","1-2","2-3","3-4","4-5","5-6","6-7","7-8","8-9","9-10","10+")
ssv_bins <- c(0, 100, 200, 300, 400, 500, 600, Inf)
ssv_labels <- c("0-100","100-200","200-300","300-400","400-500","500-600","600+")
all_interval_labels <- c(f_labels, ssv_labels)

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

  if (!ok || !file.exists(dest) || is.na(file.info(dest)$size) || file.info(dest)$size <= 0) {
    return(NULL)
  }

  dest
}

coalesce_name <- function(nms, candidates) {
  hit <- intersect(candidates, nms)
  if (length(hit) > 0) hit[[1]] else NA_character_
}

records_to_tibble <- function(records) {
  if (is.null(records) || length(records) == 0) {
    return(tibble())
  }

  # Varianta: seznam pojmenovanych objektu / data frame
  if (is.data.frame(records)) {
    df <- as_tibble(records)
    station_col <- coalesce_name(names(df), c("STATION", "station"))
    dt_col      <- coalesce_name(names(df), c("DT", "dt", "DATE", "date", "datetime"))
    element_col <- coalesce_name(names(df), c("ELEMENT", "element"))
    val_col     <- coalesce_name(names(df), c("VAL", "val", "VALUE", "value"))

    if (!any(is.na(c(station_col, dt_col, element_col, val_col)))) {
      return(df %>%
        transmute(
          STATION = as.character(.data[[station_col]]),
          DT = as.character(.data[[dt_col]]),
          ELEMENT = as.character(.data[[element_col]]),
          VAL = suppressWarnings(as.numeric(.data[[val_col]]))
        ))
    }
  }

  # Varianta: pole poli ve tvaru [STATION, ELEMENT, DT, VAL, ...]
  if (is.list(records) && length(records) > 0) {
    rows <- purrr::map(records, function(rec) {
      if (is.null(rec)) return(NULL)

      if (is.atomic(rec) && length(rec) >= 4) {
        return(tibble(
          STATION = as.character(rec[[1]]),
          ELEMENT = as.character(rec[[2]]),
          DT = as.character(rec[[3]]),
          VAL = suppressWarnings(as.numeric(rec[[4]]))
        ))
      }

      if (is.list(rec) || is.data.frame(rec)) {
        one <- records_to_tibble(rec)
        if (nrow(one) > 0) return(one)
      }

      NULL
    })

    rows <- purrr::compact(rows)
    if (length(rows) > 0) {
      return(bind_rows(rows))
    }
  }

  tibble()
}

extract_data_section <- function(x) {
  if (is.null(x)) return(NULL)

  if (is.data.frame(x)) return(x)

  if (is.list(x)) {
    for (nm in c("data", "result", "results", "features", "items")) {
      if (!is.null(x[[nm]])) {
        return(x[[nm]])
      }
    }
  }

  x
}

read_month_data <- function(path) {
  raw <- jsonlite::fromJSON(path, simplifyVector = FALSE)
  payload <- extract_data_section(raw)
  tbl <- records_to_tibble(payload)

  if (nrow(tbl) == 0) {
    stop(sprintf(
      paste(
        "V souboru %s nebyla nalezena zadna pouzitelna data.",
        "JSON zrejme neni ve formatu pojmenovanych poli, ale jako pole zaznamu.",
        "Skript je ted pripraven jak na objektovy format, tak na format [STATION, ELEMENT, DT, VAL, ...]."
      ),
      basename(path)
    ))
  }

  tbl
}

parse_dt_fields <- function(dt_char) {
  x <- str_trim(as.character(dt_char))

  dt_parsed <- suppressWarnings(as.POSIXct(
    x,
    tz = "UTC",
    tryFormats = c(
      "%Y-%m-%dT%H:%M:%OSZ",
      "%Y-%m-%dT%H:%M:%SZ",
      "%Y-%m-%dT%H:%M:%S",
      "%Y-%m-%d %H:%M:%S",
      "%Y-%m-%dT%H:%M",
      "%Y-%m-%d %H:%M",
      "%Y%m%d%H%M%S",
      "%Y%m%d%H%M"
    )
  ))

  tibble(
    YEAR = suppressWarnings(as.integer(format(dt_parsed, "%Y"))),
    MONTH = suppressWarnings(as.integer(format(dt_parsed, "%m"))),
    TIME = ifelse(is.na(dt_parsed), NA_character_, format(dt_parsed, "%H:%M"))
  )
}

assign_interval <- function(value, element) {
  out <- rep(NA_character_, length(value))

  idx_f <- which(element == "F" & !is.na(value) & value >= 0)
  if (length(idx_f) > 0) {
    out[idx_f] <- as.character(cut(
      value[idx_f],
      breaks = f_bins,
      labels = f_labels,
      right = FALSE,
      include.lowest = TRUE
    ))
  }

  idx_ssv <- which(element == "SSV10M" & !is.na(value) & value >= 0)
  if (length(idx_ssv) > 0) {
    out[idx_ssv] <- as.character(cut(
      value[idx_ssv],
      breaks = ssv_bins,
      labels = ssv_labels,
      right = FALSE,
      include.lowest = TRUE
    ))
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
    mutate(
      STATION = str_replace(as.character(STATION), "^0-20000-0-", "")
    ) %>%
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
    tidyr::complete(
      STATION,
      YEAR,
      MONTH,
      TIME,
      ELEMENT,
      INTERVAL = all_interval_labels,
      fill = list(COUNT = 0)
    ) %>%
    filter(
      (ELEMENT == "F" & INTERVAL %in% f_labels) |
      (ELEMENT == "SSV10M" & INTERVAL %in% ssv_labels)
    ) %>%
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
