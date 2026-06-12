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

  names(x) <- trimws(names(x))

  if (!all(c("WSI", "YEAR", "CALM") %in% names(x))) {
    cat("Přeskakuji, chybí WSI/YEAR/CALM:", f, "\n")
    print(names(x))
    next
  }

  x$YEAR <- as.integer(x$YEAR)
  x$CALM <- as.numeric(gsub(",", ".", x$CALM))

  x <- subset(x, YEAR >= 1991 & YEAR <= 2020)

  if (nrow(x) == 0) next

  station_mean <- aggregate(
    CALM ~ WSI,
    data = x,
    FUN = function(z) mean(z, na.rm = TRUE)
  )

  names(station_mean)[2] <- "CALM_MEAN"

  station_n <- aggregate(
    CALM ~ WSI,
    data = x,
    FUN = function(z) sum(!is.na(z))
  )

  names(station_n)[2] <- "N_YEARS"

  station_result <- merge(station_mean, station_n, by = "WSI")

  result <- rbind(result, station_result)
}

if (nrow(result) == 0) {
  stop("Nebyla načtena žádná použitelná data pro období 1991–2020.")
}

out_mean <- aggregate(
  CALM_MEAN ~ WSI,
  data = result,
  FUN = mean
)

out_n <- aggregate(
  N_YEARS ~ WSI,
  data = result,
  FUN = max
)

out <- merge(out_mean, out_n, by = "WSI")
out <- out[order(-out$CALM_MEAN), ]

write.csv(
  out,
  "wind_roses_WR08_calm_1991_2020.csv",
  row.names = FALSE
)

cat("Výstup uložen do wind_roses_WR08_calm_1991_2020.csv\n")
print(head(out, 20))
