source("R/00_libraries.R")
source("R/01_utils.R")
source("R/04_modelling_utils.R")

# get all the data --------------------------------------------------------

con <- database_connection(
  db = "modelling_sql_area"
)

tbl_name <- "[BNSSG\\Sebastian.Fox].[dbo.frontier]"

all_metrics <- tbl(
  con,
  in_schema(
    sql("modelling_sql_area"),
    sql(tbl_name)
  )
) |>
  select(
    "date",
    "metric_name",
    "org_name",
    "value"
  ) |>
  collect() |>
  mutate(
    date_day = as.Date(date)
  ) |>
  distinct()


dbDisconnect(con)

system_opel_scores <- all_metrics |>
  filter(
    metric_name == "Aggregated NHSE OPEL Score",
    org_name %in% c("Bristol Royal Infirmary", "Weston", "North Bristol NHS Trust")
  ) |>
  filter(!is.na(date)) |>
  mutate(
    weighting = case_when(
      org_name == "Bristol Royal Infirmary" ~ 0.516,
      org_name == "Weston" ~ 0.178,
      org_name == "North Bristol NHS Trust" ~ 0.306,
      .default = NA_real_
    )
  ) |> #filter(as.Date(date) == as.Date("2024-06-13")) |> #View()
  summarise(
    opel_aggregate = sum(value * weighting),
    num_orgs = n(),
    .by = c(
      date,
      metric_name
    )
  ) |>
  filter(num_orgs == 3) |>
  summarise_by_time(
    .date_var = date,
    .by = "3 hours",
    opel_aggregate  = max(opel_aggregate, na.rm = TRUE)
  ) |>
  summarise_by_time(
    .date_var = date,
    .by = "3 hours",
    opel_max  = max(opel_aggregate, na.rm = TRUE)
  ) |>
  timetk::pad_by_time(
    date, .by = "3 hours") |>
  arrange(date)

predictor_metrics <- all_metrics |>
  filter(
    !grepl("OPEL", metric_name)
  ) |>
  filter(!is.na(date)) |>
  group_by(org_name, metric_name) |>
  summarise_by_time(
    .date_var = date,
    .by = "3 hours",
    max  = max(value, na.rm = TRUE)
  ) |>
  ungroup() |>
  mutate(
    metric_name = paste(metric_name, org_name, sep = " - "),
    .keep = "unused"
  ) |>
  pivot_wider(
    names_from = metric_name,
    values_from = max
  )


modelling_matrix <- left_join(
  system_opel_scores,
  predictor_metrics,
  by = join_by(date)
)


# visualise the time series -----------------------------------------------

system_opel_scores |>
  timetk::plot_time_series(
    .date_var = date,
    .value = opel_max
  )

# modelling ---------------------------------------------------------------
debugonce(model_opel_aggregate)
modelling_outputs <- modelling_matrix |>
  filter(
    !is.na(opel_max)
    ) |>
  model_opel_aggregate()

modelling_outputs$train_test_vis
modelling_outputs$model_accuracy
modelling_outputs$forecast_plot
