source("R/00_libraries.R")
source("R/01_utils.R")

# get all the data --------------------------------------------------------

con <- database_connection(
  db = "modelling_sql_area"
)

tbl_name <- "[BNSSG\\Sebastian.Fox].[dbo.frontier]"

all_data <- tbl(
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
  )

dbDisconnect(con)


# records by date ---------------------------------------------------------

date_counts <- all_data |>
  summarise(
    records = n(),
    .by = c(
      date_day,
      metric_name,
      org_name
    )
  ) |>
  arrange(date_day, metric_name, org_name)

# plot of all records by date ---------------------------------------------

date_counts |>
  filter(!is.na(date_day)) |>
  summarise(
    across(
      records,
      sum
    ),
    .by = date_day
  ) |>
  mutate(
    year = year(date_day),
    month = month(date_day, label = TRUE),
    wkday = fct_relevel(wday(date_day, label=TRUE),
                        c("Mon", "Tue","Wed","Thu","Fri","Sat","Sun")
    ),
    day = day(date_day),
    wk = format(date_day, "%W")
  ) |>
  ggplot(
    aes(
      x = wk,
      y = wkday,
      fill = records
    )
  ) +
  geom_tile(
    colour = "black"
  ) +
  labs(
    x = "",
    y = ""
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
  scale_fill_viridis_c()

# interpretation: There are more records over time. From April 2024 onwards, it
# looks fairly consistent day to day. The first set of stable daily records is
# from April 2023, then there is a jump at the end of August 2024, then again
# from April 2024.

# There are some gaps in data:
# patchy prior to June 2023
# mid-August 2023 (for 5 days)
# Jan 2024 (8 days)
# March/April 2024 (9 days)


## UNDERSTANDING METRICS BETTER

## Thoughts: I want to model severe system pressure in the coming "time period".
## Input data might be hourly, every 15 minutes, daily, cumulative, how do these
## interplay and how do I put them into a model? Maybe I don't need to do all
## this detail and I can just focus on getting a few metrics in there and seeing
## if I can get a model built?

# plot of metric records over time ----------------------------------------

metrics_over_time <- function(data, metrics = NULL) {
  all_data <- data |>
    summarise(
      records = n(),
      .by = c(
        date_day,
        metric_name
      )
    )

  if (!is.null(metrics)) {
    all_data <- all_data |>
      filter(
        metric_name %in% metrics
      )
  }

  all_data |>
    ggplot(
      aes(
        x = date_day,
        y = metric_name
      )
    ) +
    geom_tile(
      aes(fill = records)
    ) +
    scale_fill_viridis_c() +
    theme_minimal() +
    labs(
      y = "",
      x = ""
    )
}

metrics_over_time(all_data)

weekday_metrics <- all_data |>
  mutate(
    weekday = wday(date_day, week_start = 1) %in% 1:5
  ) |>
  summarise(
    proportion = sum(weekday) / n(),
    .by = metric_name
  ) |>
  filter(proportion > 0.8) |>
  pull(metric_name)

metric_groups <- expand.grid(
  date_day = seq(
    from = as.Date("2023-04-01"),
    to = as.Date("2024-06-30"),
    by = "days"
  ),
  metric_name = unique(all_data$metric_name)
) |>
  left_join(
    all_data |>
      summarise(
        records = n(),
        .by = c(
          date_day,
          metric_name
        )
      ),
    by = join_by(
      date_day,
      metric_name
    )
  ) |>
  mutate(
    post_1st_april_2024 = ifelse(date_day > as.Date("2024-04-01"), TRUE, FALSE)
  ) |>
  summarise(
    proportion_days = sum(!is.na(records)) / n(),
    proportion_days_apr_24 = sum(!is.na(records) & post_1st_april_2024) /
      sum(post_1st_april_2024),
    .by = metric_name
  ) |>
  mutate(
    group_name = case_when(
      metric_name %in% weekday_metrics ~ "Weekday metric",
      proportion_days > 0.9 ~ "Long time series",
      proportion_days > 0.5 ~ "Medium length time series",
      proportion_days_apr_24 > 0.9 ~ "Short length time series",
      .default = "Patchy"
    )
  )

# Interpretation:
# 1. Some metrics are consistent from the start (other than gaps where there are gaps int he full dataset)
metric_groups |>
  filter(
    group_name == "Long time series"
  ) |>
  pull(metric_name) |>
  (\(x) metrics_over_time(all_data, x))() +
  labs(
    title = "Metrics with a long time series"
  )

# 2. Some metrics have a complete, but shorter, time series
metric_groups |>
  filter(
    group_name == "Medium length time series"
  ) |>
  pull(metric_name) |>
  (\(x) metrics_over_time(all_data, x))() +
  labs(
    title = "Metrics with a medium length time series"
  )

# 3. Some are short length time series
metric_groups |>
  filter(
    group_name == "Short length time series"
  ) |>
  pull(metric_name) |>
  (\(x) metrics_over_time(all_data, x))() +
  labs(
    title = "Metrics with a short length time series"
  )

# 4. Some are weekday metrics
metric_groups |>
  filter(
    group_name == "Weekday metric"
  ) |>
  pull(metric_name) |>
  (\(x) metrics_over_time(all_data, x))() +
  labs(
    title = "Metrics that the majority of records are on weekdays"
  )

# 5. Some are patchy metrics
metric_groups |>
  filter(
    group_name == "Patchy"
  ) |>
  pull(metric_name) |>
  (\(x) metrics_over_time(all_data, x))() +
  labs(
    title = "Metrics with a patchy frequency"
  )


# metrics with one value per day ------------------------------------------

daily_metrics <- all_data |>
  mutate(
    one_record_in_day = n() == 1,
    .by = c(
      date_day,
      metric_name,
      org_name
    )
  ) |>
  summarise(
    percent_true = sum(one_record_in_day) / n(),
    .by = metric_name
  ) |>
  arrange(desc(percent_true)) |>
  filter(
    percent_true > 0.95 # there are a few occasions where the daily metrics have duplicate values
  ) |>
  pull(
    metric_name
  )

# this shows the daily metrics that have duplicate values (31 occasions)
# I think the "later" value is the correct one

all_data |>
  filter(
    metric_name %in% daily_metrics
  ) |>
  filter(
    n() > 1,
    .by = c(
      date_day,
      org_name,
      metric_name
    )
  ) |>
  arrange(
    date_day,
    org_name,
    metric_name
  ) |>
  count(
    date_day,
    org_name,
    metric_name,
    value
  ) |>
  filter(n != 2) |>
  View()

# this is a record level dataset for daily metrics
daily_metrics_record_level <- all_data |>
  filter(
    metric_name %in% daily_metrics
  ) |>
  filter(
    date == max(date),
    .by = c(
      date_day,
      org_name,
      metric_name
    )
  )



# identify cumulative metrics ---------------------------------------------
# (this doesn't really work at the moment)
cumulative_metrics <- all_data |>
  # remove the daily metrics
  filter(
    !(metric_name %in% daily_metrics)
  ) |>
  arrange(
    metric_name,
    org_name,
    date
  ) |>
  # remove days where a metric only has one record for an organisation (but these aren't daily metrics)
  filter(
    n() != 1,
    .by = c(
      date_day,
      org_name,
      metric_name
    )
  ) |>
  # highlight records where there was a drop compared with a previous value
  mutate(
    decrease_on_previous = value < lag(value),
    equal_previous = value == lag(value),
    .by = c(
      date_day,
      org_name,
      metric_name
    )
  ) |>
  summarise(
    days_with_decrease = sum(decrease_on_previous, na.rm = TRUE) > 0,
    proportion_equal = sum(equal_previous, na.rm = TRUE) / (n() - 1),
    .by = c(
      date_day,
      metric_name,
      org_name
    )
  ) |>
  summarise(
    mean_proportion_with_no_increase = mean(days_with_decrease == FALSE),
    mean_proportion_equal = mean(proportion_equal),
    .by = c(
      metric_name
    )
  ) |>
  arrange(
    desc(
      mean_proportion_with_no_increase
    )
  ) |>
  filter(
    mean_proportion_with_no_increase > 0.99
  ) |>
  pull(
    metric_name
  )

all_data |>
  filter(
    grepl("\\(Since Midnight", metric_name),
    # metric_name %in% "Ambulance Handovers 60mins (Since Midnight)",
    date_day %in% sample(date_day, 10)
  ) |># View()
  ggplot(
    aes(
      x = date,
      y = value
    )
  ) +
  geom_point(
    aes(col = org_name)
  ) +
  facet_grid(
    cols = vars(date_day),
    rows = vars(metric_name),
    scales = "free"
  )


# understanding orgs and metrics ------------------------------------------

all_data |>
  count(
    org_name, metric_name
  ) |>
  ggplot(
    aes(
      x = org_name,
      y = metric_name,
      fill = n
    )
  ) +
  geom_tile() +
  scale_fill_viridis_c() +
  theme_minimal()


### NOTE, it would be good to compare the OPEL metric with Rich's breach metrics
# See table 1 in the pdf document in the "background" folder for how the OPEL parameter is calculated


# compare OPEL with current SSP metric ------------------------------------

breaches <- calculate_binary_breach_timeseries(
  date_start = as.Date("2023-03-28"),
  date_end = as.Date("2024-07-01"),
  method = 1,
  ae_param = 420,
  swast_param = 10
)

opel_metrics <- all_data |>
  filter(
    metric_name %in% c("Aggregated NHSE OPEL Score", "Automated OPEL")
  )


bnssg_opel <- opel_metrics |>
  filter(
    metric_name == "Aggregated NHSE OPEL Score"
  ) |>
  tidyr::complete(
    tidyr::nesting(date, date_day),
    metric_name,
    org_name
  ) |>
  arrange(
    org_name,
    date
  ) |>
  group_by(
    org_name
  ) |>
  tidyr::fill(
    value,
    .direction = "down"
  ) |>
  ungroup() |>
  distinct() |>
  mutate(
    weighting = case_when(
      org_name == "Bristol Royal Infirmary" ~ 0.516,
      org_name == "Weston" ~ 0.178,
      org_name == "North Bristol NHS Trust" ~ 0.306,
      .default = NA_real_
    )
  ) |>
  summarise(
    opel_aggregate = sum(value * weighting),
    .by = c(
      date,
      date_day,
      metric_name
    )
  ) |>
  mutate(
    org_name = "BNSSG calculated",
    opel_score = case_when(
      opel_aggregate <= 11 ~ 1,
      opel_aggregate <= 22 ~ 2,
      opel_aggregate <= 33 ~ 3,
      opel_aggregate <= 44 ~ 4,
      .default = NA_real_
    )
  ) |>
  select(!c("metric_name")) |>
  tidyr::pivot_longer(
    cols = starts_with("opel"),
    names_to = "metric_name",
    values_to = "value"
  )

# plot the opel aggregated score for each Trust and also the calculated score
# for BNSSG as a total; overlay this with the breach metric that Rich and others
# came up with
opel_metrics |>
  distinct() |>
  bind_rows(
    bnssg_opel
  ) |>
  left_join(
    breaches,
    by = join_by(
      dplyr::closest(
        date >= time
      )
    )
  ) |>
  select(
    !c("date_day", "time")
  ) |>
  mutate(
    metric_name = case_match(
      metric_name,
      c("opel_aggregate", "Aggregated NHSE OPEL Score") ~ "opel_aggregate",
      c("opel_score", "Automated OPEL") ~ "opel_score"
    )
  ) |>
  tidyr::pivot_wider(
    names_from = metric_name,
    values_from = value
  ) |>
  ggplot(
    aes(
      x = date,
      y = opel_aggregate
    )
  ) +
  geom_line(
    aes(group = org_name)
  ) +
  geom_point(
    aes(
      colour = factor(breach)
    )
  ) +
  annotate(
    "rect",
    xmin = min(opel_metrics$date),
    xmax = max(opel_metrics$date),
    ymin = 0,
    ymax = 11.5,
    alpha = 0.1,
    fill = "blue"

  ) +
  annotate(
    "rect",
    xmin = min(opel_metrics$date),
    xmax = max(opel_metrics$date),
    ymin = 11.5,
    ymax = 22.5,
    alpha = 0.2,
    fill = "blue"

  ) +
  annotate(
    "rect",
    xmin = min(opel_metrics$date),
    xmax = max(opel_metrics$date),
    ymin = 22.5,
    ymax = 33.5,
    alpha = 0.3,
    fill = "blue"

  ) +
  annotate(
    "rect",
    xmin = min(opel_metrics$date),
    xmax = max(opel_metrics$date),
    ymin = 33.5,
    ymax = 44,
    alpha = 0.4,
    fill = "blue"
  ) +
  theme_minimal() +
  labs(
    title = "Automated aggregate OPEL score compared with specified breach metric",
    y = "Aggregate OPEL score",
    x = ""
  ) +
  facet_wrap(
    facets = vars(org_name)
  ) +
  scale_colour_manual(
    name = "Breach (based on A&E & SWAST thresholds)",
    values = c(
      `TRUE` = "red",
      `FALSE` = "black"
    )
  ) +
  theme(
    legend.position = "bottom"
  )

# Q: do the calculated OPEL scores from the individual trusts equal the BNSSG
# OPEL score provided by frontier?

all_data |>
  filter(
    org_name == "NHS Bristol, North Somerset, South Gloucestershire Integrated Care Board",
    metric_name == "OPEL"
  ) |>
  rename(
    value_in_database = "value"
  ) |>
  select(
    !c("metric_name", "org_name", "date_day")
  ) |>
  inner_join(
    bnssg_opel,
    by = join_by(
      closest(date >= date)
    )
  ) |>
  filter(
    metric_name == "opel_score"
  ) %$%
  table(
    value_in_database,
    value
  )

# A: On most occasions, yes, but not always!

# Q: how long are the time series for the 9 metrics that make up the aggregate
# OPEL score?

all_data |>
  filter(
    grepl("^[[:digit:]]\\.", metric_name)
  ) |>
  count(
    org_name, metric_name
  ) |>
  View()

###############
orgs_total <- date_counts |>
  summarise(
    across(
      records,
      sum
    ),
    .by = org_name
  ) |>
  arrange(
    records
  )

date_counts |>
  summarise(
    records = sum(records),
    .by = c(
      metric_name,
      date
    )
  ) |>
  ggplot(
    aes(x = date,
        y = records)
  ) +
  geom_col() +
  scale_x_date() +
  theme_bw() +
  facet_wrap(
    facets = vars(metric_name),
    scales = "free_y"
  )

earliest_by_org <- date_counts |>
  summarise(
    earliest_date = min(date),
    .by = org_name
  )


# looking at the smaller metrics ------------------------------------------

fewest_metrics <- metrics_total |>
  filter(records == min(records)) |>
  pull(metric_name)

con <- database_connection()

tbl_name <- "[BNSSG\\Sebastian.Fox].[dbo.frontier]"

metric_records <- tbl(
  con,
  in_schema(
    sql("modelling_sql_area"),
    sql(tbl_name)
  )
) |>
  filter(
    metric_name %in% fewest_metrics
  ) |>
  select(
    "date",
    "metric_name",
    "org_name",
    "value"
  ) |>
  collect()

dbDisconnect(con)


# Does trust data add up to BNSSG data ------------------------------------

con <- database_connection()

tbl_name <- "[BNSSG\\Sebastian.Fox].[dbo.frontier]"

ambulance_to_hosp <- tbl(
  con,
  in_schema(
    sql("modelling_sql_area"),
    sql(tbl_name)
  )
) |>
  filter(
    metric_name == "Ambulances En Route to Hospitals",
    date == as.Date("2024-03-01")
  ) |>
  select(
    "date",
    "metric_name",
    "org_name",
    "value"
  ) |>
  collect()

dbDisconnect(con)





# view data by the minute it is recorded ----------------------------------

all_data |>
  mutate(
    minute = lubridate::minute(date)
  ) |>
  count(
    minute
  ) |>
  arrange(
    minute
  )

# # A tibble: 5 × 2
# minute       n
# <int>   <int>
#   1      0 2328261
# 2     15 1450721
# 3     30 2305603
# 4     45 1468711
# 5     NA      48


# summarise daily ----------------------------------------------------------

daily <- all_data |>
  mutate(
    date = as.Date(date)
  ) |>
  summarise(
    across(
      value,
      list(
        mean = mean,
        min = min,
        max = max,
        med = median
      )
    ),
    .by = c(
      date,
      metric_name,
      org_name
    )
  ) |>
  filter(
    !is.na(date)
  ) |>
  tidyr::pivot_longer(
    cols = starts_with("value"),
    names_to = "statistic",
    names_prefix = "value_",
    values_to = "value"
  )

# plot every metric over time for every org -------------------------------



metrics <- unique(daily$metric_name)

pdf(
  paste0("tests/data_checks/all-metrics-time-series.pdf"),
  width = 12,
  height = 8
)

for (metric in metrics) {

  p <- daily |>
    filter(
      metric_name == metric
    ) |>
    ggplot(
      aes(x = date)
    ) +
    # geom_line(
    #   aes(
    #     y = value,
    #     group = org_name,
    #     colour = statistic
    #   ),
    #   na.rm = TRUE
    # ) +
    geom_point(
      aes(
        y = value,
        colour = statistic
      )
    ) +
    facet_wrap(
      facets = vars(org_name),
      scales = "free_y"
    ) +
    theme_minimal() +
    # scale_x_continuous(
    #   expand = expansion(0),
    #   breaks = function(x) seq(x[1], x[2], by = 1)
    # ) +
    labs(
      title = metric,
      y = "Value",
      x = ""
    ) +
    theme(
      axis.text.x = element_text(
        angle = 45,
        hjust = 1
      )
    )
  print(p)


}
dev.off()


# do "since midnight" metrics just increase through the day ---------------

all_data |>
  filter(
    grepl("since midnight", metric_name, ignore.case = TRUE),
    between(
      date,
      as.Date("2024-06-01"),
      as.Date("2024-06-15")
    )
  ) |>
  filter(
    org_name == "North Bristol NHS Trust"
  ) |>
  ggplot(
    aes(
      x = date,
      y = value
    )
  ) +
  geom_point() +
  facet_wrap(
    facets = vars(metric_name),
    scales = "free_y"

  )




all_data |>
  filter(
    metric_name == "NBT P1 NCtR Beddays"
  ) |>
  summarise(
    val = sum(value),
    .by = org_name
  )
