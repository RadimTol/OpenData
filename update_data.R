options(timeout = 120)

library(stringr)
library(dplyr)
library(lubridate)
library(jsonlite)
library(tidyr)

# -----------------------------
# 1) Přehled více adresářů -> data.json
# -----------------------------
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

  cat("Zpracovávám přehled:", source_name, "\n")

  txt <- readLines(url, warn = FALSE)

  files <- unlist(str_extract_all(txt, "[-0-9A-Za-z]+\\.json"))
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
  arrange(date, source) |>
  mutate(date = as.character(date))

write_json(result, "data.json", pretty = TRUE, auto_unbox = TRUE)
cat("Zapsáno: data.json\n")

# -----------------------------
# 2) Podrobná analýza now/data po dnech a hodinách -> now_hourly.json
# -----------------------------
now_url <- "https://opendata.chmi.cz/meteorology/climate/now/data/"
cat("Zpracovávám hodinovou analýzu NOW\n")

txt_now <- readLines(now_url, warn = FALSE)

# řádky s json soubory
json_lines <- txt_now[str_detect(txt_now, "\\.json")]

# V Apache indexu je řádek typicky:
# 10m-...-20260318.json    18-Mar-2026 09:02   67152
now_df <- tibble(line = json_lines) |>
  mutate(
    file = str_extract(line, "[-0-9A-Za-z]+\\.json"),
    dt_str = str_extract(line, "\\d{2}-[A-Za-z]{3}-\\d{4}\\s+\\d{2}:\\d{2}")
  ) |>
  filter(!is.na(file), !is.na(dt_str)) |>
  mutate(
    dt = as.POSIXct(dt_str, format = "%d-%b-%Y %H:%M", tz = "Europe/Prague"),
    day = as.Date(dt),
    hour = as.integer(format(dt, "%H"))
  )

# jen 3 dny, které jsou v adresáři NOW aktuálně přítomné
days_present <- sort(unique(now_df$day))

now_hourly <- now_df |>
  count(day, hour, name = "n_files") |>
  complete(day = days_present, hour = 0:23, fill = list(n_files = 0)) |>
  arrange(day, hour) |>
  mutate(day = as.character(day))

write_json(now_hourly, "now_hourly.json", pretty = TRUE, auto_unbox = TRUE)
cat("Zapsáno: now_hourly.json\n")
