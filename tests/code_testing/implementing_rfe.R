# dummy example to implement recursive feature elimination with a recipe

source("R/00_libraries.R")
library(modeldata)
library(caret)

df <- m4_monthly |>
  mutate(
    value = case_when(
      value > quantile(value, 0.9) ~ "high",
      .default = "low"
    ),
    value = factor(value),
    .by = id
  )

splits <- initial_validation_time_split(df)
data_train <- training(splits)
data_test <- testing(splits)
val_set <- validation_set(splits)

# # recursive feature elimination
# ctrl <- rfeControl(
#   functions = lmFuncs,
#   method = "repeatedcv",
#   repeats = 5,
#   verbose = FALSE
# )
#
# lmProfile <- rfe(
#   x = data_train |> select(where(is.numeric)),
#   y = data_train |> pull("Status"),
#   sizes = 1:5,
#   rfeControl = ctrl,
#   na.action = na.omit,
#   metric = "Accuracy"
# )
#
# predictors(lmProfile)

predictors <- setdiff(names(data_train), c("date", "value"))

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
    value,
    new_role = "outcome"
  ) |>
  update_role(
    all_of(predictors),
    new_role = "predictor"
  ) |>
  step_date(
    date
  ) |>
  step_rm(
    date
  ) |>
  # step_unknown(
  #   all_nominal_predictors()
  # ) |>
  # step_impute_median(
  #   all_numeric_predictors()
  # ) |>
  # step_naomit(
  #   Status
  # ) |>
  step_dummy(
    recipes::all_nominal_predictors()
  ) |>
  step_nzv(recipes::all_predictors()) |>
  step_corr()

# recursive feature elimination

baked_cols <- rec |>
  prep() |>
  bake(new_data = NULL) |> #summary()
  ncol()
start <- Sys.time()
ctrl <- caret::rfeControl(
  functions = caret::lrFuncs,
  method = "cv",
  repeats = 5,
  verbose = FALSE,
  rerank = TRUE
)
# debugonce(rfe)
lrProfile <- caret::rfe(
  rec,
  data = data_train,
  sizes = 2:baked_cols,
  rfeControl = ctrl,
  # na.action = na.omit,
  metric = "Accuracy"
)
Sys.time() - start
plot(lrProfile, type=c("g", "o"))

post_prep_predictors <- rec |>
  prep() |>
  summary() |>
  filter(role == "predictor") |>
  pull(variable)

rec <- rec |>
  update_role(
    all_of(setdiff(
      post_prep_predictors,
      caret::predictors(lrProfile)
    )),
    new_role = "recursive feature elimination"
  )


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
