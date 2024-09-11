# dummy example to implement recursive feature elimination with a recipe

source("R/00_libraries.R")
library(modeldata)
library(caret)

df <- credit_data

splits <- initial_validation_split(df)
data_train <- training(splits)
data_test <- testing(splits)
val_set <- validation_set(splits)

# recursive feature elimination
ctrl <- rfeControl(
  functions = lmFuncs,
  method = "repeatedcv",
  repeats = 5,
  verbose = FALSE
)

lmProfile <- rfe(
  x = data_train |> select(where(is.numeric)),
  y = data_train |> pull("Status"),
  sizes = 1:5,
  rfeControl = ctrl,
  na.action = na.omit,
  metric = "Accuracy"
)

predictors(lmProfile)

predictors <- setdiff(names(data_train), "Status")

cores <- parallel::detectCores()

model_engine <- logistic_reg(
  penalty = tune(),
  mixture = tune()
) |>
  set_engine(
    "glmnet",
    num.threads = !!cores
  )

rec <- recipe(x = data_train) |>
  update_role(
    Status,
    new_role = "outcome"
  ) |>
  update_role(
    all_of(predictors),
    new_role = "predictor"
  ) |>
  step_unknown(
    all_nominal_predictors()
  ) |>
  step_impute_median(
    all_numeric_predictors()
  ) |>
  step_naomit(
    Status
  ) |>
  step_dummy(all_nominal_predictors())

wflw <- workflow() |>
  add_model(model_engine) |>
  add_recipe(rec)

residuals <- tune::tune_grid(
  object = wflw,
  resamples = val_set,
  grid = 15,
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

residuals |> collect_metrics() |> inner_join(best)
model_fit |> collect_metrics()
