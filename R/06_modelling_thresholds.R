source("R/00_libraries.R")
source("R/01_utils.R")
source("R/04_modelling_utils.R")

# prepare the data --------------------------------------------------------

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

frontier_three_hour_data <- all_metrics |>
  filter(!is.na(date)) |>
  group_by(
    org_name, metric_name
  ) |>
  summarise_by_time(
    .date_var = date,
    .by = "3 hours",
    value  = last(value)
  ) |>
  ungroup() |>
  mutate(
    metric_name = paste0(
      metric_name,
      " (",
      org_name,
      ")"
    ),
    .keep = "unused"
  ) |>
  pivot_wider(
    names_from = metric_name,
    values_from = value
  ) |>
  arrange(date) |>
  timetk::pad_by_time(
    .date_var = date,
    .by = "3 hours"#,
    # .fill_na_direction = "downup"
  )

breaches <- calculate_binary_breach_timeseries(
  date_start = as.Date(min(frontier_three_hour_data$date)),
  date_end = as.Date(max(frontier_three_hour_data$date)),
  method = 1,
  ae_param = 420,
  swast_param = 10
) |>
  filter(
    lubridate::hour(time) == 0
  ) |>
  mutate(
    time = as.POSIXct(time, format="%Y-%m-%d %H:%M:%OS")
  )

# final data

modelling_data <- right_join(
  breaches,
  frontier_three_hour_data,
  by = join_by(time == date)
) |>
  arrange(time) |>
  tidyr::fill(
    breach,
    .direction = "up"
  ) |>
  filter(!is.na(breach)) |>
  mutate(
    date = as.Date(time - 1),
    breach = factor(ifelse(breach==TRUE, 1, 0)),
    .keep = "unused"
  ) |>
  tidyr::pivot_longer(
    cols = !c("date", "breach"),
    names_to = "metric",
    values_to = "value",
    values_drop_na = TRUE
  ) |>
  summarise(
    across(
      value,
      list(
        mean = ~ mean(.x, na.rm = TRUE),
        min = ~ min(.x, na.rm = TRUE),
        max = ~ max(.x, na.rm = TRUE)
      ),
      .names = "{.fn}"
    ),
    .by = c(date, metric, breach)
  ) |>
  tidyr::complete(
    nesting(date, breach), metric
  ) |>
  fill(
    mean,
    .direction="downup"
  ) |>
  fill(
    min,
    .direction="downup"
  ) |>
  fill(
    max,
    .direction="downup"
  ) |>
  pivot_longer(
    cols = !c("date", "metric", "breach"),
    names_to = "stat",
    values_to = "value"
  ) |>
  mutate(
    metric = paste(metric, stat),
    .keep = "unused"
  )




rm(all_metrics, frontier_three_hour_data, breaches)

start_time <- Sys.time()

best_models <- 1:10 |>
  purrr::map(
    ~ model_function(
      modelling_data,
      model_day = .x,
      grid_search = 30,
      model_type = "rf",
      auto_feature_selection = FALSE
    )
  )

Sys.time() - start_time

val_rocs <- purrr::map(
  best_models,
  ~ purrr::pluck(.x, "val", "roc")
) |>
  patchwork::wrap_plots(
    ncol = 5
  )

test_rocs <- purrr::map(
  best_models,
  ~ purrr::pluck(.x, "test", "roc")
) |>
  patchwork::wrap_plots(
    ncol = 5
  )

ggsave(
  val_rocs,
  filename = "outputs/validation_rocs_rf.png",
  width = 12,
  height = 8,
  units = "in",
  bg = NA
)

ggsave(
  test_rocs,
  filename = "outputs/test_rocs_rf.png",
  width = 12,
  height = 8,
  units = "in",
  bg = NA
)
debugonce(visualise_model_predictions)


select_best_model <- function(best_model_by_day) {
  if ("mixture" %in% names(best_model_by_day)) {
    bm <- best_model_by_day |>
      filter(
        .metric == "roc_auc",
        mean == max(mean),
        .by = .metric
      ) |>
      filter(
        mixture == max(mixture)
      )
  } else if ("trees" %in% names(best_model_by_day)) {
    bm <- best_model_by_day |>
      filter(
        .metric == "roc_auc",
        mean == max(mean),
        .by = .metric
      ) |>
      filter(
        mtry == min(mtry)
      )
  }
}


test_set_predictions <- purrr::map(
  1:10,
  ~ visualise_model_predictions(
    best_model_test_predictions = best_models[[.x]]$test$predictions,
    best_model_val_predictions = best_models[[.x]]$val$predictions |>
      inner_join(
        select_best_model(
          best_models[[.x]]$val$metrics
        )#,
        # by = join_by(
        #   .config, penalty, mixture
        # )
      ),
    modelling_data,
    model_day = .x
  )
) |>
  patchwork::wrap_plots(
    ncol = 5,
    guides = "collect"
  )

ggsave(
  test_set_predictions,
  filename = "outputs/test_prediction_status_rf.png",
  width = 18,
  height = 5,
  units = "in",
  bg = NA
)
