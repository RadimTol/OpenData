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

base_data_url <- "https://opendata.chmi.cz/meteorology/climate/now/data/"
base_meta_url <- "https://opendata.chmi.cz/meteorology/climate/now/metadata/"

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

get_today_files_from_index <- function(index_url, today = Sys.Date()) {
  page <- rvest::read_html(index_url)
  txt  <- rvest::html_text2(page)

  lines <- unlist(strsplit(txt, "\n", fixed = TRUE))
  lines <- trimws(lines)
  lines <- lines[nzchar(lines)]

  today_str <- format(today, "%d-%b-%Y")

  hits <- stringr::str_subset(
    lines,
    paste0("^.+\\.json\\s+", today_str, "\\s+\\d{2}:\\d{2}\\s+\\d+$")
  )

  if (length(hits) == 0) {
    return(character(0))
  }

  file_names <- stringr::str_match(
    hits,
    "^(.+?\\.json)\\s+\\d{2}-[A-Za-z]{3}-\\d{4}\\s+\\d{2}:\\d{2}\\s+\\d+$"
  )[, 2]

  file_names <- trimws(file_names)
  unique(paste0(index_url, file_names))
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
      GH_ID     = as.character(GH_ID),
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

read_now_data <- function(file_urls) {
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
      dplyr::mutate(
        STATION = as.character(STATION),
        ELEMENT = as.character(ELEMENT),
        DT = suppressWarnings(lubridate::ymd_hms(DT, tz = "UTC", quiet = TRUE)),
        VAL = suppressWarnings(as.numeric(VAL))
      ) |>
      dplyr::filter(ELEMENT %in% c("T", "TMA", "TMI")) |>
      dplyr::filter(!is.na(DT), !is.na(VAL))
  })
}

get_latest_per_station_element <- function(df) {
  df |>
    dplyr::arrange(STATION, ELEMENT, dplyr::desc(DT)) |>
    dplyr::group_by(STATION, ELEMENT) |>
    dplyr::slice(1) |>
    dplyr::ungroup()
}

get_top_bottom3 <- function(df_latest, station_meta) {
  joined <- df_latest |>
    dplyr::left_join(
      station_meta |> dplyr::select(WSI, GH_ID, NAME, ELEVATION),
      by = c("STATION" = "WSI")
    )

  highest <- joined |>
    dplyr::group_by(ELEMENT) |>
    dplyr::arrange(dplyr::desc(VAL), dplyr::desc(DT), .by_group = TRUE) |>
    dplyr::slice_head(n = 3) |>
    dplyr::mutate(EXTREME = "MAX", RANK = dplyr::row_number()) |>
    dplyr::ungroup()

  lowest <- joined |>
    dplyr::group_by(ELEMENT) |>
    dplyr::arrange(VAL, dplyr::desc(DT), .by_group = TRUE) |>
    dplyr::slice_head(n = 3) |>
    dplyr::mutate(EXTREME = "MIN", RANK = dplyr::row_number()) |>
    dplyr::ungroup()

  dplyr::bind_rows(highest, lowest) |>
    dplyr::select(ELEMENT, EXTREME, RANK, STATION, GH_ID, NAME, ELEVATION, DT, VAL) |>
    dplyr::arrange(ELEMENT, EXTREME, RANK)
}

# ------------------------------------------------------------
# Hlavní běh
# ------------------------------------------------------------

today <- Sys.Date()

data_files_all <- get_today_files_from_index(base_data_url, today)

if (length(data_files_all) == 0) {
  stop("Nenalezeny dnešní JSON soubory v ", base_data_url)
}

meta2_url <- get_latest_file_by_pattern(base_meta_url, "^meta2-\\d{8}\\.json$")
meta1_url <- get_latest_file_by_pattern(base_meta_url, "^meta1-\\d{8}\\.json$")

wsi_tbl <- get_wsi_for_elements(meta2_url, c("T", "TMA", "TMI"))
data_files <- filter_files_by_wsi(data_files_all, wsi_tbl$WSI)

if (length(data_files) == 0) {
  stop("Po filtrování přes meta2 nezbyly žádné dnešní soubory.")
}

station_meta <- prepare_station_metadata(meta1_url)
raw_data <- read_now_data(data_files)

if (nrow(raw_data) == 0) {
  stop("V načtených souborech nejsou žádná data T/TMA/TMI.")
}

latest_vals <- get_latest_per_station_element(raw_data)
result <- get_top_bottom3(latest_vals, station_meta)

readr::write_excel_csv(result, "MaxMinT.csv", na = "")

message("Hotovo: MaxMinT.csv")
print(result)
