base_url <- "https://opendata.chmi.cz/meteorology/climate/historical_csv/data/yearly/wind_rose/"

# načtení seznamu souborů
html <- readLines(base_url, warn = FALSE)

files <- unique(sub('.*href="([^"]*WR08[^"]*\\.csv)".*',
                    '\\1',
                    grep("WR08.*\\.csv", html, value = TRUE)))

result <- data.frame()

for (f in files) {

  cat("Čtu", f, "\n")

  url <- paste0(base_url, f)

  x <- tryCatch(
    read.csv(url, stringsAsFactors = FALSE),
    error = function(e) NULL
  )

  if (is.null(x))
    next

  if (!all(c("WSI", "YEAR", "CALM") %in% names(x)))
    next

  x <- subset(x, YEAR >= 1991 & YEAR <= 2020)

  if (nrow(x) == 0)
    next

  station_mean <- aggregate(
    CALM ~ WSI,
    data = x,
    FUN = function(z) mean(z, na.rm = TRUE)
  )

  names(station_mean)[2] <- "CALM_MEAN"

  result <- rbind(result, station_mean)
}

# pokud je WSI ve více souborech
out <- aggregate(
  CALM_MEAN ~ WSI,
  data = result,
  FUN = mean
)

out <- out[order(-out$CALM_MEAN), ]

write.csv(
  out,
  "wind_s_WR08_calm_1991_2020.csv",
  row.names = FALSE
)

print(out)
