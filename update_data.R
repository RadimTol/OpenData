options(timeout = 120)

library(stringr)
library(dplyr)
library(lubridate)
library(jsonlite)

urls <- c(
  "https://opendata.chmi.cz/meteorology/climate/now/data/",
  "https://opendata.chmi.cz/meteorology/climate/recent/data/1hour",
  "https://opendata.chmi.cz/meteorology/climate/recent/data/10min",
  "https://opendata.chmi.cz/meteorology/climate/recent/data/daily",
  "https://opendata.chmi.cz/meteorology/climate/recent/data/phenomena"
)

names(urls) <- c("now", "1hour", "10min", "daily", "phenomena")

all_data <- list()

for (i in seq_along(urls)) {
  url <- urls[i]
  source_name <- names(urls)[i]

  cat("Zpracovávám:", source_name, "\n")

  txt <- readLines(url, warn = FALSE)

  files <- unlist(str_extract_all(txt, "[-0-9]+\\.json"))
  files <- unique(files)

  df <- data.frame(file = files) |>
    mutate(
      date_str = str_extract(file, "\\d{8}(?=\\.json$)|\\d{6}(?=\\.json$)"),
      date = case_when(
        str_length(date_str) == 8 ~ as.Date(date_str, format = "%Y%m%d"),
        str_length(date_str) == 6 ~ as.Date(paste0(date_str, "01"), format = "%Y%m%d"),
        TRUE ~ as.Date(NA)
      ),
      source = source_name
    ) |>
    filter(!is.na(date)) |>
    count(date, source, name = "n_files")

  all_data[[i]] <- df
}

result <- bind_rows(all_data) |>
  arrange(date, source)

write_json(result, "data.json", pretty = TRUE, auto_unbox = TRUE)

cat("Hotovo, zapsáno do data.json\n")