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

target_elements <- c("T", "SRA1H", "Fmax")
output_file <- "histnow.csv"
tz_local <- "Europe/Prague"

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

get_wsi_for_elements <- function(meta2_url, elements = target_elements) {
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

read_now_data <- function(file_urls, elements = target_elements) {
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
      dplyr::filter(ELEMENT %in% elements) |>
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

make_histogram_table <- function(df, run_time_local, bins = 10) {
  purrr::map_dfr(sort(unique(df$ELEMENT)), function(el) {
    sub <- df |>
      dplyr::filter(ELEMENT == el)

    x <- sub$VAL
    x <- x[is.finite(x)]

    if (length(x) == 0) {
      return(tibble())
    }

    xmin <- min(x)
    xmax <- max(x)

    if (xmin == xmax) {
      out <- tibble(
        run_datetime_local = format(run_time_local, "%Y-%m-%d %H:%M:%S %Z"),
        source = "NOW",
        element = el,
        value_datetime_utc = format(max(sub$DT, na.rm = TRUE), "%Y-%m-%d %H:%M:%S UTC"),
        bin_no = 1:bins,
        bin_left = rep(xmin, bins),
        bin_right = rep(xmax, bins),
        count = c(length(x), rep(0L, bins - 1))
      )
      return(out)
    }

    breaks <- seq(xmin, xmax, length.out = bins + 1)
    h <- hist(x, breaks = breaks, plot = FALSE, include.lowest = TRUE, right = TRUE)

    tibble(
      run_datetime_local = format(run_time_local, "%Y-%m-%d %H:%M:%S %Z"),
      source = "NOW",
      element = el,
      value_datetime_utc = format(max(sub$DT, na.rm = TRUE), "%Y-%m-%d %H:%M:%S UTC"),
      bin_no = seq_len(length(h$counts)),
      bin_left = h$breaks[-length(h$breaks)],
      bin_right = h$breaks[-1],
      count = as.integer(h$counts)
    )
  })
}

# ------------------------------------------------------------
# Hlavní běh
# ------------------------------------------------------------

run_time_local <- with_tz(Sys.time(), tz_local)
today <- Sys.Date()

data_files_all <- get_today_files_from_index(base_data_url, today)

if (length(data_files_all) == 0) {
  stop("Nenalezeny dnešní JSON soubory v ", base_data_url)
}

meta2_url <- get_latest_file_by_pattern(base_meta_url, "^meta2-\\d{8}\\.json$")
wsi_tbl <- get_wsi_for_elements(meta2_url, target_elements)

data_files <- filter_files_by_wsi(data_files_all, wsi_tbl$WSI)

if (length(data_files) == 0) {
  stop("Po filtrování přes meta2 nezbyly žádné dnešní soubory.")
}

raw_data <- read_now_data(data_files, target_elements)

if (nrow(raw_data) == 0) {
  stop("V načtených souborech nejsou žádná data pro požadované prvky.")
}

latest_vals <- get_latest_per_station_element(raw_data)

if (nrow(latest_vals) == 0) {
  stop("Nepodařilo se vybrat nejaktuálnější hodnoty pro stanice a prvky.")
}

# ------------------------------------------------------------
# Uložení vstupních dat pro histogram
# ------------------------------------------------------------

# readr::write_excel_csv(
#  latest_vals,
#  "datanow.csv",
#  na = ""
# )

# message("Uložena vstupní data: datanow.csv")

result <- make_histogram_table(latest_vals, run_time_local, bins = 10)

if (nrow(result) == 0) {
  stop("Histogram se nepodařilo vytvořit, nejsou k dispozici hodnoty.")
}

readr::write_excel_csv(result, output_file, na = "")
message("Hotovo: ", output_file)
print(result)
