
# install.packages(c(
#   "jsonlite", "dplyr", "purrr", "stringr", "readr",
#   "lubridate", "rvest", "xml2", "tibble"
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

base_data_url <- "https://opendata.chmi.cz/meteorology/climate/now/data/"
base_meta_url <- "https://opendata.chmi.cz/meteorology/climate/now/metadata/"

# ------------------------------------------------------------
# Pomocné funkce
# ------------------------------------------------------------

parse_chmi_json_values <- function(url) {
  x <- jsonlite::fromJSON(url, simplifyVector = FALSE)

  # očekávaná cesta podle veřejného JSON formátu ČHMÚ
  vals <- x$data$data$values
  hdr  <- x$data$data$header

  if (is.null(vals)) {
    stop("V JSON nebylo nalezeno x$data$data$values: ", url)
  }

  # hlavička je obvykle "STATION,ELEMENT,DT,VAL,FLAG,QUALITY"
  col_names <- str_split(hdr, ",", simplify = TRUE) |>
    as.character() |>
    trimws()

  out <- as_tibble(do.call(rbind, vals), .name_repair = "minimal")
  names(out) <- col_names[seq_len(ncol(out))]

  out
}

get_today_files_from_index <- function(index_url, today = Sys.Date()) {
  page <- read_html(index_url)
  txt  <- html_text2(page)

  # Apache-style index, řádky typu:
  # 10m-...json   19-Mar-2026 00:02   74971
  lines <- unlist(strsplit(txt, "\n", fixed = TRUE))
  lines <- trimws(lines)
  lines <- lines[nzchar(lines)]

  today_str <- format(today, "%d-%b-%Y")

  # bereme jen JSON soubory v /now/data/ vytvořené dnes
  hits <- str_subset(
    lines,
    paste0("^.+\\.json\\s+", today_str, "\\s+\\d{2}:\\d{2}\\s+\\d+$")
  )

  if (length(hits) == 0) {
    return(character(0))
  }

  file_names <- str_match(hits, "^(.+?\\.json)\\s+\\d{2}-[A-Za-z]{3}-\\d{4}\\s+\\d{2}:\\d{2}\\s+\\d+$")[, 2]
  file_names <- trimws(file_names)

  # necháme jen 10m soubory, protože T/TMA/TMI jsou podle popisu mezi 10min prvky
  file_names <- file_names[str_detect(file_names, "^10m-.*\\.json$")]

  paste0(index_url, file_names)
}

get_meta1_url_for_today <- function(today = Sys.Date()) {
  paste0(base_meta_url, "meta1-", format(today, "%Y%m%d"), ".json")
}

prepare_station_metadata <- function(meta_url) {
  meta_raw <- parse_chmi_json_values(meta_url)

  # V meta1 je podle dokumentace FULL_NAME, nikoli NAME.
  # Pro výstup ho přejmenujeme na NAME.
  wanted <- c("GH_ID", "FULL_NAME", "ELEVATION")

  missing_cols <- setdiff(wanted, names(meta_raw))
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

read_today_data <- function(file_urls) {
  if (length(file_urls) == 0) {
    return(tibble(
      STATION = character(),
      ELEMENT = character(),
      DT = character(),
      VAL = numeric(),
      FLAG = character(),
      QUALITY = numeric()
    ))
  }

  purrr::map_dfr(file_urls, function(u) {
    message("Načítám: ", u)

    df <- parse_chmi_json_values(u)

    needed <- c("STATION", "ELEMENT", "DT", "VAL")
    missing_cols <- setdiff(needed, names(df))
    if (length(missing_cols) > 0) {
      stop("V datovém souboru chybí očekávané sloupce: ",
           paste(missing_cols, collapse = ", "),
           " | soubor: ", u)
    }

    df |>
      mutate(
        STATION = as.character(STATION),
        ELEMENT = as.character(ELEMENT),
        DT = ymd_hms(DT, tz = "UTC", quiet = TRUE),
        VAL = suppressWarnings(as.numeric(VAL))
      ) |>
      filter(ELEMENT %in% c("T", "TMA", "TMI")) |>
      filter(!is.na(DT), !is.na(VAL))
  })
}

get_latest_per_station_element <- function(df) {
  df |>
    arrange(STATION, ELEMENT, desc(DT)) |>
    group_by(STATION, ELEMENT) |>
    slice(1) |>
    ungroup()
}

get_top_bottom3 <- function(df_latest, station_meta) {
  joined <- df_latest |>
    left_join(station_meta, by = "STATION")

  highest <- joined |>
    group_by(ELEMENT) |>
    arrange(desc(VAL), desc(DT), .by_group = TRUE) |>
    slice_head(n = 3) |>
    mutate(EXTREME = "MAX", RANK = row_number()) |>
    ungroup()

  lowest <- joined |>
    group_by(ELEMENT) |>
    arrange(VAL, desc(DT), .by_group = TRUE) |>
    slice_head(n = 3) |>
    mutate(EXTREME = "MIN", RANK = row_number()) |>
    ungroup()

  bind_rows(highest, lowest) |>
    select(ELEMENT, EXTREME, RANK, STATION, NAME, ELEVATION, DT, VAL) |>
    arrange(ELEMENT, EXTREME, RANK)
}

# ------------------------------------------------------------
# Hlavní běh
# ------------------------------------------------------------

today <- Sys.Date()

# 1) soubory vytvořené dnes v /now/data/
today_files <- get_today_files_from_index(base_data_url, today)

if (length(today_files) == 0) {
  stop("V ", base_data_url, " nebyly nalezeny žádné 10m JSON soubory vytvořené dnes.")
}

# 2) dnešní meta1
meta_url <- get_meta1_url_for_today(today)
station_meta <- prepare_station_metadata(meta_url)

# 3) načtení dnešních souborů
raw_data <- read_today_data(today_files)

if (nrow(raw_data) == 0) {
  stop("V dnešních souborech nebyla nalezena data pro ELEMENT = T, TMA, TMI.")
}

# 4) nejaktuálnější hodnota pro každou stanici a element
latest_vals <- get_latest_per_station_element(raw_data)

# 5) TOP 3 a BOTTOM 3
result <- get_top_bottom3(latest_vals, station_meta)

# 6) výstup
readr::write_excel_csv(result, "MaxMinT.csv", na = "")

message("Hotovo. Výstup uložen do MaxMinT.csv")
print(result)
