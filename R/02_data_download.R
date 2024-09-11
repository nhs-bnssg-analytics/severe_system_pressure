source("R/00_libraries.R")
source("R/01_utils.R")

## Lookups for metric/org
metric_lookup <- read.csv('data-raw/metrics.csv') |>
  dplyr::select(code, metric_name = name)
org_lookup <- read.csv('data-raw/organisations.csv')  |>
  dplyr::select(code, org_name = name)

## chunk up the api request:

# have data from 1st Jan 2023
start_date  <- lubridate::ymd("2021-01-01")
end_date  <- lubridate::ymd("2022-12-31")
chunk_size  <- 3


start_dates <- seq.Date(
  from = start_date,
  to = end_date - lubridate::ddays(chunk_size - 1),
  by = glue::glue("{chunk_size} days")
)

end_dates <- seq.Date(
  from = start_date + lubridate::ddays(chunk_size - 1),
  to = end_date,
  by = glue::glue("{chunk_size} days")
)

# force the final date to be the end_date
end_dates <- replace(
  x = end_dates,
  list = end_dates == max(end_dates),
  end_date
)


start_time <- Sys.time()
# get the data from the API
df_list <- purrr::map2(
  start_dates,
  end_dates,
  ~ get_api_data(
    .x,
    .y,
    metric_lookup = metric_lookup,
    org_lookup = org_lookup
  )
)

df <- df_list |>
  purrr::list_rbind() |>
  filter(
    metric_name != "Comment"
  ) |>
  mutate(
    value = as.numeric(value)
  )

print(Sys.time() - start_time)


# upload the data to SQL --------------------------------------------------


create_append_table(
  table_name = "dbo.frontier",
  data = df
)


# remove_table("dbo.frontier")
