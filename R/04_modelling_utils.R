source("R/00_libraries.R")
source("R/01_utils.R")

model_opel_aggregate <- function(data) {

  # split the data
  splits <- timetk::time_series_split(
    data,
    date_var = date,
    assess = "10 days",
    initial = "28 days",
    cumulative = FALSE
  )


  # visualise

  train_test_vis <- splits |>
    timetk::tk_time_series_cv_plan() |>
    plot_time_series_cv_plan(
      .date_var = date,
      .value = opel_max,
      .interactive = FALSE
    )

  # create train test
  data_train <- training(splits)
  data_test <- testing(splits)


  # splits_test <- timetk::time_series_cv(
  #   opel_scores,
  #   date_var = date,
  #   initial = "4 weeks",
  #   assess = "10 days",
  #   cumulative = FALSE#,
  #   # slice_limit = 5,
  #   # skip = 7
  # )


  # create recipe
  recipe_spec <- recipe(
    opel_max ~ .,
    data = data_train
  ) |>
    step_timeseries_signature(date) |>
    step_holiday_signature(
      date,
      exchange_set = "LONDON"
    ) |> # consider bank holidays
    step_normalize(date_index.num) |>
    step_zv(all_predictors()) |>
    step_dummy(all_nominal_predictors(), one_hot = TRUE) |>
    step_rm(date, all_nominal()) |>
    step_impute_median(
      all_predictors()
    )

  # model specs:

  # lm
  # model_lm <- linear_reg() |>
  #   set_engine(
  #     "lm"
  #   )
  #
  # wflw_lm <- workflow() |>
  #   add_recipe(recipe_spec) |>
  #   add_model(model_lm) |>
  #   fit(data_train)

  # random_forest see R/modeltime-parallel-processing.R for setting up multiple
  # models with different tuning parameters
  model_rf <- rand_forest(
    mode = "regression"
  ) |>
    set_engine(
      "ranger"
    )

  wflw_rf <- workflow() |>
    add_recipe(recipe_spec) |>
    add_model(model_rf) |>
    fit(data_train)


# create modeltime table --------------------------------------------------

  models_tbl <- modeltime::modeltime_table(
    # wflw_lm,
    wflw_rf
  )


  # model calibration on test set -------------------------------------------

  calibration_tbl <- models_tbl |>
    modeltime::modeltime_calibrate(
      new_data = data_test,
      quiet = FALSE
    )

  # accuracy assessment -----------------------------------------------------

  model_accuracy <- calibration_tbl |>
    modeltime_accuracy() #|>
    # table_modeltime_accuracy(
    #   .interactive = interactive
    # )

  # visualise the forecast test ---------------------------------------------

  forecast_plot <- calibration_tbl |>
    modeltime_forecast(
      new_data = data_test,
      actual_data = data #|> filter(Time > as.Date("2014-04-01"))
    ) |>
    plot_modeltime_forecast(
      .legend_max_width = 25, # For mobile screens
      .interactive      = T
    )

  output <- list(
    train_test_vis = train_test_vis,
    model_accuracy = model_accuracy,
    forecast_plot = forecast_plot
  )

  return(output)
}

lag_function <- function(data, lags) {
  multilag <- function(x, lags) {
    names(lags) <- as.character(lags)
    purrr::map_dfr(lags, lag, x = x)
  }

  data <- data |>
    tidyr::pivot_wider(
      names_from = metric,
      values_from = value
    ) |>
    mutate(
      across(
        !c(date, breach),
         ~multilag(.x, lags),
        .unpack = TRUE
      ),
      .keep = "unused"
    )

  return(data)
}


model_function <- function(data, model_day, grid_search = 1, model_type) {

  model_type <- match.arg(
    model_type,
    c("glmnet", "rf")
  )

  data <- data |>
    lag_function(lags = model_day:(model_day + 6)) |>
    slice(-(1:(model_day + 6)))

  splits <- initial_validation_time_split(data)
  val_set <- validation_set(splits)

  predictors <- setdiff(names(data), c("date", "breach"))


  recipe <- recipe(
    x = training(splits)
  ) |>
    step_date(
      date,
      features = c("dow", "month")
    ) |>
    step_rm(date) |>
    update_role(breach, new_role = "outcome") |>
    update_role(all_of(predictors), new_role = "predictor") |>
    step_zv(all_numeric_predictors()) |>
    step_pca(
      # num_comp = tune()
      all_numeric_predictors(),
      num_comp = 20
    )


  cores <- parallel::detectCores()

  if (model_type == "glmnet") {
    model_engine <- logistic_reg(
      penalty = tune(),
      mixture = tune()
    ) |>
      set_engine(
        "glmnet",
        num.threads = !!cores
      )

    recipe <- recipe |>
      step_normalize(all_predictors())

    # grid <-
    #   dials::parameters(
    #     dials::num_comp(c(1, 9)),
    #     dials::penalty(),
    #     dials::mixture()
    #   ) %>%
    #   dials::grid_regular(levels = c(4, 10, 10)) %>%
    #   arrange(num_terms, penalty, mixture)

  } else if (model_type == "rf") {
    model_engine <- rand_forest(
      mtry = tune(),
      trees = tune(),
      min_n = tune()
    ) |>
      set_engine(
        "ranger",
        num.threads = !!cores
      ) |>
      set_mode("classification")

    # browser()
    # grid <-
    #   dials::parameters(
    #     dials::num_comp(c(1, 9)),
    #     dials::mtry(),
    #     dials::trees(),
    #     dials::min_n()
    #   ) %>%
    #   dials::grid_regular(levels = c(4, 5, 10, 7)) %>%
    #   arrange(num_terms, mtry, trees, min_n)
  }

  recipe <- recipe |>
    step_dummy(
      all_nominal_predictors()
    )

  wflw <- workflow() |>
    add_model(model_engine) |>
    add_recipe(recipe)



  residuals <- tune::tune_grid(
    object = wflw,
    resamples = val_set,
    grid = grid_search,
    control = tune::control_grid(
      save_pred = TRUE,
      save_workflow = FALSE
    )
  )

  best <- tune::select_best(
    residuals,
    metric = "roc_auc"
  )


  wflw_final <- wflw |>
    tune::finalize_workflow(best)


  model_fit <- last_fit(
    wflw_final,
    splits,
    add_validation_set = TRUE,
    metrics = yardstick::metric_set(
      yardstick::roc_auc
    )
  )

  draw_roc_curves <- function(fit, metrics, grid_config) {
    plot <- fit |>
      collect_predictions() |>
      filter(.config == grid_config) |>
      yardstick::roc_curve(
        .pred_0,
        truth = breach
      ) |>
      ggplot(aes(x = 1 - specificity, y = sensitivity)) +
      geom_path() +
      geom_abline(lty = 3) +
      coord_equal() +
      theme_bw() +
      labs(
        title = paste("Day", model_day, "model"),
        subtitle = paste(
          "AUC:",
          round(
            metrics |> filter(.config == grid_config) |> pull(.estimate),
            digits = 2)
        )
      )
  }

  validation_metrics <- residuals |>
    collect_metrics()


  test_metrics <- model_fit |>
    collect_metrics()


  val_metrics <- validation_metrics |>
    filter(
      .metric == "roc_auc"
    ) |>
    rename(
      .estimate = "mean"
    )

  val_predictions <- residuals |>
    collect_predictions()

  test_predictions <- model_fit |>
    collect_predictions()

  grid_configs <- unique(best$.config)

  validation_roc <- draw_roc_curves(residuals, val_metrics, grid_configs)

  test_roc <- draw_roc_curves(model_fit, test_metrics, unique(test_metrics$.config))


  return(list(
    val = list(
      metrics = validation_metrics,
      roc = validation_roc,
      predictions = val_predictions
    ),
    test = list(
      metrics = test_metrics,
      roc = test_roc,
      predictions = test_predictions
    )
  ))
}


# modelling understanding -------------------------------------------------

#' returns a calendar heatmap showing how predictions compare with observed
visualise_model_predictions <- function(best_model_predictions, modelling_data, model_day) {
  row_range <- range(best_model_predictions$.row)

  heatmap <- modelling_data |>
    distinct(
      date, breach
    ) |>
    slice(-(1:7)) |>
    filter(
      between(
        row_number(),
        row_range[1],
        row_range[2]
      )
    ) |>
    rename(
      breach_inputs = "breach"
    ) |>
    bind_cols(
      best_model_predictions
    ) |>
    mutate(
      breach_predicted = case_when(
        .pred_0 > 0.5 ~ 0,
        .default = 1
      ),
      status = case_when(

      )
    )

  return(heatmap)
}
