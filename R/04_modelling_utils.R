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
         ~ multilag(.x, lags),
        .unpack = TRUE
      ),
      .keep = "unused"
    )

  return(data)
}


model_function <- function(data, model_day, grid_search = 1, model_type, auto_feature_selection = TRUE) {

  cat(paste("Day", model_day, "\n"))
  model_type <- match.arg(
    model_type,
    c("glmnet", "rf")
  )

  data <- data |>
    lag_function(lags = model_day:(model_day + 6)) |>
    slice(-(1:(model_day + 6)))

  splits <- initial_validation_time_split(data) # I think we need to do this because when we're applying the model it will be applied chronologically, like the validation hyperparameter tuning
  # splits <- initial_validation_split(data)
  val_set <- validation_set(splits)

  # predictors <- setdiff(names(data), c("date", "breach"))


  recipe <- recipe(
    breach ~ .,
    data = training(splits)
  ) |>
    step_date(
      date,
      features = c("dow", "month"),
      role = "date_features"
    ) |>
    step_rm(date) |>
    # update_role(breach, new_role = "outcome") |>
    # update_role(all_of(predictors), new_role = "predictor") |>
    step_nzv(recipes::all_numeric_predictors()) |>
    step_corr(recipes::all_numeric_predictors())

  if (model_type == "glmnet") {
    recipe <- recipe |>
      step_normalize(
        recipes::all_numeric_predictors()
      )
  }

  cores <- parallel::detectCores()

  if (isTRUE(auto_feature_selection)) {
    tm <- Sys.time()
    cat("...feature elimination....")
    tm <- log_the_time(tm)
    cat("...counting baked cols....")
    baked_cols <- recipe |>
      prep()

    predictors <- baked_cols |>
      summary() |>
      filter(role == "predictor") |>
      pull(variable)

    baked_cols <- length(predictors)

    tm <- log_the_time(tm)
    # cat("...rfeControl stage....")
    # ctrl <- caret::rfeControl(
    #   functions = caret::lrFuncs,
    #   method = "repeatedcv",
    #   repeats = 5,
    #   verbose = FALSE,
    #   rerank = TRUE
    # )
    cat("...sbfControl stage....")
    ctrl <- caret::sbfControl(
      functions = caret::lrFuncs,
      method = "repeatedcv",
      repeats = 5,
      verbose = FALSE
    )
# browser()
    tm <- log_the_time(tm)
    # cat("...parallelising....")
    # cl <- parallel::makeCluster(
    #   2,
    #   type = 'PSOCK'
    # )
    # doParallel::registerDoParallel(cl)

    # debugonce(rfe)
    tm <- log_the_time(tm)
    cat("...feature elimination stage....")
    browser()
    # lrProfile <- caret::rfe(
    #   recipe,
    #   data = training(splits),
    #   sizes = seq(
    #     from = 50,
    #     to = baked_cols,
    #     by = 100
    #   ),
    #   rfeControl = ctrl,
    #   metric = "Accuracy"
    # )

    lr_with_filter <- caret::sbf(
      recipe,
      data = training(splits),
      sbfControl = ctrl
    )

    tm <- log_the_time(tm)
    cat("...updating the recipe....")
    recipe <- recipe |>
      update_role(
        all_of(setdiff(
          predictors,
          predictors(lrProfile)
        )),
        new_role = "recursive feature elimination"
      )
  } else if (auto_feature_selection == FALSE) tm <- Sys.time()

  if (model_type == "glmnet") {
    cat("...logistic regression...")
    model_engine <- logistic_reg(
      penalty = tune(),
      mixture = tune()
    ) |>
      set_engine(
        "glmnet",
        family = stats::binomial(link = "logit"),
        num.threads = !!cores
      ) |>
      set_mode(
        "classification"
      )



  } else if (model_type == "rf") {
    cat("...random forest...")
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
      # recipes::all_nominal_predictors()
      recipes::has_role("date_features")
    )

  if (model_type == "glmnet") {
    complete_params <- NULL
  } else if (model_type == "rf") {
    params <- tribble(
        ~parameter, ~object,
        "mtry", dials::mtry(),
        "trees", dials::trees(),
        "min_n", dials::min_n()
      )

    complete_params <- params |>
      mutate(object = purrr::map(
        object,
        dials::finalize,
        recipe |> prep() |> bake(new_data = NULL)
      )
    ) |>
      pull(object) |>
      dials::parameters()
  }

  wflw <- workflow() |>
    add_model(model_engine) |>
    add_recipe(recipe)
# browser()
  tm <- log_the_time(tm)
  cat("...tuning hyperparameters....")
  residuals <- tune::tune_grid(
    object = wflw,
    resamples = val_set,
    grid = grid_search,
    param_info = complete_params,
    control = tune::control_grid(
      parallel_over = "resamples",
      save_pred = TRUE,
      save_workflow = FALSE
    )
  )

  best <- tune::select_best(
    residuals,
    metric = "roc_auc"
  )


  tm <- log_the_time(tm)
  cat("...finalising workflow....")
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

  tm <- log_the_time(tm)
  cat("...understanding performance....")
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

  tm <- log_the_time(tm)

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
visualise_model_predictions <- function(best_model_test_predictions, best_model_val_predictions, modelling_data, model_day) {

  threshold <- probably::threshold_perf(
    best_model_val_predictions,
    breach,
    `.pred_0`,
    thresholds = seq(0, 1, 0.01)
  ) |>
    filter(.metric == "j_index") |>
    filter(.estimate == max(.estimate)) |>
    pull(.threshold) |>
    mean()

  heatmap <- modelling_data |>
    distinct(
      date, breach
    ) |>
    slice(-(1:7)) |>
    mutate(
    .row = row_number()
    ) |>
    rename(
      breach_inputs = "breach"
    ) |>
    inner_join(
      best_model_test_predictions,
      by = join_by(.row)
    ) |>
    mutate(
      breach_predicted = case_when(
        .pred_0 > threshold ~ 0,
        .default = 1
      ),
      status = case_when(
        breach_predicted == 1 & breach_inputs == 1 ~ "True positive",
        breach_predicted == 1 & breach_inputs == 0 ~ "False positive",
        breach_predicted == 0 & breach_inputs == 1 ~ "False negative",
        .default = "True negative"
      ),
      status = factor(
        status,
        levels = c(
          "True positive",
          "True negative",
          "False positive",
          "False negative"
        )),
      wk = lubridate::week(date),
      wkday = lubridate::wday(date, label = TRUE, week_start = 1),
      month = lubridate::month(date, label = TRUE),
      year = lubridate::year(date)
    ) |>
    ggplot(
      aes(
        x = wk,
        y = wkday,
        fill = status
      )
    ) +
    geom_tile(
      colour = "black",
      aes(
        alpha = status
      ),
      show.legend = TRUE
    ) +
    labs(
      x = "",
      y = "",
      title = paste("Day", model_day)
    ) +
    theme(
      panel.background = element_blank(),
      axis.ticks = element_blank(),
      axis.text.x = element_blank(),
      strip.background = element_rect("grey92")
    ) +
    facet_grid(
      rows = vars(year),
      cols = vars(month),
      scales = "free",
      space = "free"
    ) +
    scale_fill_manual(
      name = "",
      values = c(
        "True positive" = "#FFC107",
        "True negative" = "#004D40",
        "False positive" = "#1E88E5",
        "False negative" = "#D81B60"
      ),
      drop = FALSE
    ) +
    scale_alpha_manual(
      name = "",
      values = c(
        "True positive" = 0.15,
        "True negative" = 0.15,
        "False positive" = 1,
        "False negative" = 1
      ),
      drop = FALSE
    )

  return(heatmap)
}

log_the_time <- function(previous_time) {
  new_time <- Sys.time()
  cat(
    paste(
      format(new_time - previous_time),
      "\n"
    )
  )
  return(new_time)
}
