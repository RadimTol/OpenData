base_url <- "https://opendata.chmi.cz/meteorology/climate/historical_csv/data/yearly/wind_rose/"

html <- readLines(base_url, warn = FALSE)

files <- regmatches(html, gregexpr('yrs-[^"]*WR08\\.csv', html))
files <- unique(unlist(files))

cat("Počet nalezených WR08 souborů:", length(files), "\n")
print(head(files))

if (length(files) == 0) {
  stop("Nebyly nalezeny žádné soubory WR08.")
}

result <- data.frame()

for (f in files) {

  cat("Čtu", f, "\n")
  url <- paste0(base_url, f)

  x <- tryCatch(
    read.csv(url, stringsAsFactors = FALSE, check.names = FALSE),
    error = function(e) {
      cat("Chyba při čtení:", f, "\n")
      print(e)
      NULL
    }
  )

  if (is.null(x)) next

  # odstranění prázdného sloupce vzniklého koncovou čárkou v CSV
  x <- x[, names(x) != "", drop = FALSE]

  names(x) <- trimws(names(x))

  if (!all(c("WSI", "YEAR", "CALM") %in% names(x))) {
    cat("Přeskakuji, chybí WSI/YEAR/CALM:", f, "\n")
    print(names(x))
    next
  }

  x$YEAR <- as.integer(x$YEAR)
  x$CALM <- as.numeric(x$CALM)

  x <- x[x$YEAR >= 1991 & x$YEAR <= 2020, ]

  if (nrow(x) == 0) next

  station_result <- data.frame(
    WSI = unique(x$WSI)[1],
    CALM_MEAN = mean(x$CALM, na.rm = TRUE),
    N_YEARS = sum(!is.na(x$CALM))
  )

  result <- rbind(result, station_result)
}

if (nrow(result) == 0) {
  stop("Nebyla načtena žádná použitelná data pro období 1991–2020.")
}

out <- result[order(-result$CALM_MEAN), ]

write.csv(
  out,
  "wind_roses_WR08_calm_1991_2020.csv",
  row.names = FALSE
)

cat("Výstup uložen do wind_roses_WR08_calm_1991_2020.csv\n")
print(head(out, 20))
