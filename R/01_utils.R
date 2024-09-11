## Get the URL for the scrape get request
make_api_url <- function(date_from, date_to, metrics = "") {
  request <- httr2::request(
    Sys.getenv("api_root")
  ) |>
    httr2::req_url_path_append(date_from) |>
    httr2::req_url_path_append(date_to)

  if (metrics != "") {
    request <- request |>
      httr2::req_url_query(
        `metric_code` = metrics,
        .multi = "explode"
      )
  }

  request <- request |>
    httr2::req_headers(
      # 'accept' = 'application/json',
      'UserAPI-Key' = httr2::secret_decrypt(
        "XpfDSQ_071RB4IyBb5ixhxUwmiUAeB4GmPb2RBa4ZaqSbdd9d3qGCvlzAbyUQ8qegGi_UKm3eYs",
        "HTTR2_KEY" # environment variable name - it is the output of httr2::secret_make_key()
      )#,
      # 'Retry_After' = 30
    ) |>
    httr2::req_user_agent("Sebastian Fox BNSSG - sebastian.fox3@nhs.net") |> # tells the API who is querying it (polite)
    httr2::req_throttle(10 / 60) # throttle the calls to 10 requests every 60 seconds

  return(request)
}

record_error_body <- function(resp) {
  resp |>
    httr2::resp_body_string()
}

# Get and format data
get_api_data <- function(date_from, date_to, metrics = "", metric_lookup, org_lookup) {

  request <- make_api_url(date_from, date_to, metrics = metrics)
  api_data <- request |>
    httr2::req_error(
      is_error = \(resp) FALSE,
      body = record_error_body
    ) |>
    httr2::req_perform()

  # browser()
  if (httr2::resp_is_error(api_data)) {
    api_data <- data.frame(
      date = .POSIXct(double()),
      metrics_id = integer(),
      metric_name = character(),
      metrics_org = character(),
      org_name = character(),
      value = character()
    )
  } else if (httr2::resp_status(api_data) == 204) {
    api_data <- data.frame(
      date = .POSIXct(double()),
      metrics_id = integer(),
      metric_name = character(),
      metrics_org = character(),
      org_name = character(),
      value = character()
    )
  } else {
    api_data <- api_data |>
      httr2::resp_body_json(
        simplifyVector = TRUE
      ) |>
      data.frame() |>
      janitor::clean_names() |>
      dplyr::right_join(
        metric_lookup,
        by = dplyr::join_by('metrics_id' == 'code')
      ) |>
      dplyr::right_join(
        org_lookup,
        by = dplyr::join_by('metrics_org' == 'code')
      ) |>
      dplyr::mutate(
        date = stringr::str_replace(metrics_dt, 'T', ' '),
        date = as.POSIXct(date, format = '%Y-%m-%d %H:%M:%S')
      ) |>
      dplyr::mutate(
        date = lubridate::floor_date(date, unit = "15 mins")
      ) |>
      dplyr::rename(value = metrics_val) |>
      dplyr::select(
        date, metrics_id, metric_name, metrics_org, org_name, value
      )

    print(max(api_data$date, na.rm = TRUE))
  }



  Sys.sleep(5)

  return(api_data)
}



# database functions ------------------------------------------------------

database_connection <- function(db) {

  con <- dbConnect(
    odbc(),
    Driver = "SQL Server",
    Server = "Xsw-00-ash01",
    # Database = "modelling_sql_area",
    Database = db,
    Trusted_Connection = "True",
    timeout = 120
  )
}

create_append_table <- function(table_name, data) {

  con <- database_connection(
    db = "modelling_sql_area"
  )

  on.exit(
    dbDisconnect(con)
  )

  table_exist <- DBI::dbExistsTable(
    conn = con,
    name = table_name
  )

  if (!isTRUE(table_exist)) {
    field_types <- c(
      "date" = "datetime",
      "metrics_id" = "integer",
      "metric_name" = "varchar(255)",
      "metrics_org" = "varchar(255)",
      "org_name" = "varchar(255)",
      "value" = "decimal"
    )

    DBI::dbCreateTable(
      conn = con,
      name = table_name,
      fields = field_types
    )
  }


  DBI::dbAppendTable(
    conn = con,
    name = table_name,
    value = data
  )

  return(invisible(TRUE))
}

remove_table <- function(table_name) {
  con <- database_connection(
    db = "modelling_sql_area"
  )

  on.exit(
    dbDisconnect(con)
  )

  DBI::dbRemoveTable(
    conn = con,
    name = table_name
  )

  return(invisible(TRUE))
}



# Calculating breaches ----------------------------------------------------

obtain_ae_data <- function(date_start, date_end) {

  # date_start_minus_two <- date_start - 2

  sq <- seq(
    from = lubridate::floor_date(as.POSIXct(date_start), "day"),
    to = lubridate::ceiling_date(as.POSIXct(date_end) + 1, "day"),
    by = 3600 * 3
  )

  con <- database_connection(
    db = "Analyst_SQL_Area"
  )

  on.exit(
    dbDisconnect(con)
  )

  tbl_name <- "[dbo].[tbl_BNSSG_ECDS]"

  ae_data <- tbl(
    con,
    in_schema(
      sql("Analyst_SQL_Area"),
      sql(tbl_name)
    )
  ) |>
    filter(
      !is.null(Decision_To_Admit_Date),
      Site %in% c("RA701", "RVJ01", "RA7C2"),
      Departure_Date <= date_end
    ) |>
    select(
      c("Site",
        "Departure_Time_Since_Arrival",
        "Departure_Date",
        "Departure_Time")
    ) |>
    collect() |>
    tidyr::drop_na() |>
    mutate(
      departure_dt = paste0(
        Departure_Date,
        " ",
        substr(Departure_Time, 1, 8)
      ),
      departure_dt = as.POSIXct(departure_dt)
    ) |>
      inner_join(
        tibble(
          end = sq,
          start = end - (3600 * 24)
        ),
        by = join_by(
          between(
            departure_dt,
            start,
            end
          )
        )
      ) |>
    summarise(
      medianval = median(Departure_Time_Since_Arrival, na.rm = TRUE),
      .by = c(end)
    ) |>
    arrange(
      end
    )


  return(ae_data)

}

obtain_ambulance_data <- function(date_start, date_end) {

  sq <- seq(
    from = lubridate::floor_date(as.POSIXct(date_start), "day"),
    to = lubridate::ceiling_date(as.POSIXct(date_end) + 1, "day"),
    by = 3600 * 3
  )

  con <- database_connection(
    db = "ABI"
  )

  on.exit(
    dbDisconnect(con)
  )

  tbl_name <- "[dbo].[vw_UrgentCare_Ambulance_SWASFT]"

  amb_data <- tbl(
    con,
    in_schema(
      sql("ABI"),
      sql(tbl_name)
    )
  ) |>
    filter(
      CCG == "15C",
      Outcome %in% c("See & Treat", "See & Convey"),
      Incident_Colour == "Category 2"
    ) |>
    select(
      start ='Response_Target_1_Clock Start',
      end ='Time_First_Core_Resource_Allocated'
    ) |>
    collect() |>
    mutate(
      start = as.POSIXct(start, format="%Y-%m-%d %H:%M:%OS"),
      end = as.POSIXct(end, format="%Y-%m-%d %H:%M:%OS"),
      duration = as.numeric((end - start) / 60)
    ) |>
    filter(
      duration >= 0
    ) |>
    select(
      c("end", "duration")
    ) |>
    inner_join(
      tibble(
        end_sq = sq,
        start = end_sq - (3600 * 24)
      ),
      by = join_by(
        between(
          end,
          start,
          end_sq
        )
      )
    ) |>
    summarise(
      medianval = median(duration, na.rm = TRUE),
      .by = c(end_sq)
    ) |>
    arrange(
      end_sq
    ) |>
    rename(
      end = "end_sq"
    )
  return(amb_data)

}

calculate_performance_timeseries <- function(date_start, date_end) {
  a_e <- obtain_ae_data(
    date_start = date_start,
    date_end = date_end
  ) |>
    mutate(
      type = "ae"
    )

  amb <- obtain_ambulance_data(
    date_start = date_start,
    date_end = date_end
  ) |>
    mutate(
      type = "swast"
    )

  performance_data <- bind_rows(
    a_e,
    amb
  ) |>
    rename(
      time = "end"
    ) |>
    mutate(
      time = as.character(
        format(
          time
        )
      )
    )

  return(performance_data)
}

identify_breaches <- function(performance_data, method, ae_param, swast_param) {

    if (method == 1) {
      performance_data <- performance_data |>
        mutate(
          breach = ifelse(
            type=="ae",
            ifelse(medianval > ae_param, 1, 0),
            ifelse(medianval > swast_param, 1, 0))
        )
    } #else if (method == 2) {
    #   fnx <- function(vals, lambda) {
    #     PeakSegDisk::PeakSegFPOP_vec(vals,lambda)$segments |>
    #       mutate(rowid=row_number()) |>
    #       mutate(dur=chromEnd-chromStart) |>
    #       uncount(dur) |>
    #       group_by(rowid) |>
    #       mutate(times=chromStart+row_number()-1) |>
    #       arrange(times) |>
    #       ungroup() |>
    #       mutate(breach=ifelse(status=="peak",1,0))  |>
    #       .$breach
    #   }
    #   dat1<-dat |>
    #     mutate(
    #       breach=c(
    #         fnx(
    #           dat |>
    #             filter(type=="ae") |>
    #             .$medianval |>
    #             as.integer,
    #           lambda = aeparam
    #         ),
    #         fnx(
    #           dat |>
    #             filter(type=="swast") |>
    #             .$medianval |>
    #             as.integer,
    #           lambda = swastparam
    #         )
    #       )
    #     )
    # }

  breach_timeseries <- performance_data |>
    select("time", "breach", "type") |>
    tidyr::pivot_wider(
      names_from = type,
      values_from = breach
    ) |>
    mutate(
      breach_and = ifelse(ae == 1 & swast == 1, TRUE, FALSE),
    ) |>
    select(
      "time",
      breach = "breach_and"
    ) |>
    mutate(
      time = as.POSIXct(
        time,
        format = '%Y-%m-%d %H:%M:%S',
        tz = "UTC"
      )
    )

    return(breach_timeseries)
}


calculate_binary_breach_timeseries <- function(date_start, date_end, method, ae_param, swast_param) {

  performance_data <- calculate_performance_timeseries(
    date_start = date_start,
    date_end = date_end
  )

  binary_timeseries <- identify_breaches(
    performance_data = performance_data,
    method = 1,
    ae_param = ae_param,
    swast_param = swast_param
  )


  return(binary_timeseries)
}
