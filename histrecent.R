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

target_elements <- c("T", "SRA", "Fmax")
sra_lag_days <- 1
output_file <- "histrecent.csv"
tz_local <- "Europe/Prague"

# ------------------------------------------------------------
# Pomocné funkce
# ------------------------------------------------------------

parse_chmi_json_values <- function(url, max_attempts = 4, wait_seconds = 10) {
  last_error <- NULL

  for (attempt in seq_len(max_attempts)) {
    result <- tryCatch({
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
    }, error = function(e) {
      last_error <<- e
      NULL
    })

    if (!is.null(result)) {
      if (attempt > 1) {
        message("JSON načten po opakování: ", basename(url), " (pokus ", attempt, "/", max_attempts, ")")
      }
      return(result)
    }

    if (attempt < max_attempts) {
      message(
        "JSON zatím nelze načíst: ", basename(url),
        " | pokus ", attempt, "/", max_attempts,
        " | čekám ", wait_seconds, " s | chyba: ", conditionMessage(last_error)
      )
      Sys.sleep(wait_seconds)
    }
  }

  stop(
    "Nepodařilo se načíst JSON ani po ", max_attempts, " pokusech: ",
    basename(url), " | URL: ", url,
    " | poslední chyba: ", conditionMessage(last_error),
    call. = FALSE
  )
}

parse_chmi_json_values_safe <- function(url, max_attempts = 4, wait_seconds = 10) {
  tryCatch(
    parse_chmi_json_values(url, max_attempts = max_attempts, wait_seconds = wait_seconds),
    error = function(e) {
      warning(
        "Přeskakuji nečitelný/nekompletní JSON po opakovaných pokusech: ",
        basename(url),
        " | URL: ", url,
        " | chyba: ", conditionMessage(e),
        call. = FALSE
      )
      tibble::tibble()
    }
  )
}

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

parse_record_date <- function(x) {
  x <- as.character(x)

  dt1 <- suppressWarnings(lubridate::ymd_hms(x, tz = "UTC", quiet = TRUE))
  d1  <- as.Date(dt1)

  d2  <- suppressWarnings(lubridate::ymd(x, quiet = TRUE))
  d3  <- suppressWarnings(as.Date(substr(x, 1, 10)))

  dplyr::coalesce(d1, d2, d3)
}

read_daily_data <- function(file_urls, elements = target_elements) {
  purrr::map_dfr(file_urls, function(u) {
    message("Načítám: ", u)

    df <- parse_chmi_json_values_safe(u)

    needed <- c("STATION", "ELEMENT", "VAL")
    missing_cols <- setdiff(needed, names(df))
    if (length(missing_cols) > 0) {
      stop("V datovém souboru chybí očekávané sloupce: ",
           paste(missing_cols, collapse = ", "),
           " | soubor: ", u)
    }

    if (!("VTYPE" %in% names(df))) {
      df$VTYPE <- NA_character_
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
        VTYPE = as.character(VTYPE),
        VAL = suppressWarnings(as.numeric(VAL)),
        RECORD_DATE = parse_record_date(.data[[date_col]])
      ) |>
      dplyr::filter(ELEMENT %in% elements) |>
      dplyr::filter(ELEMENT != "T" | VTYPE == "AVG") |>
      dplyr::filter(!is.na(RECORD_DATE), !is.na(VAL))
  })
}

get_effective_recent_data <- function(raw_data, sra_lag_days = 1) {
  non_sra_data <- raw_data |>
    dplyr::filter(ELEMENT != "SRA")

  if (nrow(non_sra_data) == 0) {
    stop("V datech nejsou žádné prvky jiné než SRA, nelze určit referenční den.")
  }

  ref_day <- max(non_sra_data$RECORD_DATE, na.rm = TRUE)
  sra_day <- ref_day - sra_lag_days

  message("Referenční den pro T/Fmax: ", format(ref_day, "%Y-%m-%d"))
  message("Použitý den pro SRA: ", format(sra_day, "%Y-%m-%d"))

  main_part <- raw_data |>
    dplyr::filter(ELEMENT != "SRA", RECORD_DATE == ref_day)

  sra_part <- raw_data |>
    dplyr::filter(ELEMENT == "SRA", RECORD_DATE == sra_day)

  dplyr::bind_rows(main_part, sra_part)
}

make_histogram_table <- function(df, run_time_local, bins = 10) {
  purrr::map_dfr(sort(unique(df$ELEMENT)), function(el) {
    x <- df |>
      dplyr::filter(ELEMENT == el) |>
      dplyr::pull(VAL)

    x <- x[is.finite(x)]

    if (length(x) == 0) {
      return(tibble())
    }

    xmin <- min(x)
    xmax <- max(x)

    if (xmin == xmax) {
      out <- tibble(
        run_datetime_local = format(run_time_local, "%Y-%m-%d %H:%M:%S %Z"),
        source = "RECENT",
        element = el,
        value_date = as.character(max(df$RECORD_DATE[df$ELEMENT == el], na.rm = TRUE)),
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
      source = "RECENT",
      element = el,
      value_date = as.character(max(df$RECORD_DATE[df$ELEMENT == el], na.rm = TRUE)),
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

data_files_all <- get_root_json_files(base_data_url)

if (length(data_files_all) == 0) {
  stop("Nenalezeny žádné JSON soubory v rootu ", base_data_url)
}

meta2_url <- get_latest_file_by_pattern(base_meta_url, "^meta2-\\d{8}\\.json$")
wsi_tbl <- get_wsi_for_elements(meta2_url, target_elements)

data_files <- filter_files_by_wsi(data_files_all, wsi_tbl$WSI)

if (length(data_files) == 0) {
  stop("Po filtrování přes meta2 nezbyly žádné daily soubory.")
}

raw_data <- read_daily_data(data_files, target_elements)

if (nrow(raw_data) == 0) {
  stop("V načtených daily souborech nejsou žádná data pro požadované prvky.")
}

effective_data <- get_effective_recent_data(raw_data, sra_lag_days = sra_lag_days)

if (nrow(effective_data) == 0) {
  stop("Po výběru referenčních dnů nezůstala žádná data.")
}

# ------------------------------------------------------------
# Uložení vstupních dat pro histogram
# ------------------------------------------------------------

# readr::write_excel_csv(
#  effective_data,
#  "datarecent.csv",
#  na = ""
# )

# message("Uložena vstupní data: datarecent.csv")

result <- make_histogram_table(effective_data, run_time_local, bins = 10)

if (nrow(result) == 0) {
  stop("Histogram se nepodařilo vytvořit, nejsou k dispozici hodnoty.")
}

readr::write_excel_csv(result, output_file, na = "")
message("Hotovo: ", output_file)
print(result)
