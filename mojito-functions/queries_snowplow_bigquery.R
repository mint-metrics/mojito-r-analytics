#
# Mojito Snowplow/BigQuery query functions
#

mojitoBqConstrctor <- function(ds, query) {
  tb <- bq_dataset_query(ds, query = query)
  return(bq_table_download(tb))
}


# Unique conversion data
mojitoGetUniqueConversions <- function(wave_params, goal, operand="=", goal_count=1, segment_type=NA, segment_value=NA, segment_val_operand="=", segment_negative=FALSE) {

  if(!is.na(segment_type)) {
    segment_clause <- paste0("
      AND x.subject ",ifelse(segment_negative, "NOT","")," IN (
        SELECT DISTINCT subject 
        FROM ",wave_params$tables$segment,"
        WHERE segment_type = '",segment_type,"' 
            AND segment_value ",segment_val_operand," '",segment_value,"'
        )")
  } else {
    segment_clause <- ""
  }

  query <- paste0(
    "WITH
      user_goal_count AS (
        SELECT 
          c.subject
        FROM ",wave_params$tables$goal," c
          INNER JOIN ",wave_params$tables$exposure," x
            ON c.subject = x.subject
              AND x.wave_id = '",wave_params$wave_id,"'
              AND x.exposure_tstamp < c.conversion_tstamp
              AND exposure_tstamp BETWEEN
                TIMESTAMP('",wave_params$start_date,"', '",mojitoReportTimezone,"')
                AND TIMESTAMP('",wave_params$stop_date,"', '",mojitoReportTimezone,"')
              AND conversion_tstamp BETWEEN
                TIMESTAMP('",wave_params$start_date,"', '",mojitoReportTimezone,"')
                AND TIMESTAMP('",wave_params$stop_date,"', '",mojitoReportTimezone,"')
              and ",goal,"
        GROUP BY 1
        HAVING count(*) >= ",goal_count,"
      ),

      daily_aggregate AS (
        SELECT
          TIMESTAMP_TRUNC(x.exposure_tstamp, ",wave_params$time_grain,", '",mojitoReportTimezone,"') as exposure_tstamp,
          x.recipe_name,
          count(DISTINCT x.subject) as subjects,
          count(DISTINCT c.subject) as conversions
        FROM ",wave_params$tables$exposure," x
          LEFT JOIN user_goal_count c
            ON (
              x.subject = c.subject
              AND exposure_tstamp BETWEEN
                TIMESTAMP('",wave_params$start_date,"', '",mojitoReportTimezone,"')
                AND TIMESTAMP('",wave_params$stop_date,"', '",mojitoReportTimezone,"')
            )
        WHERE
          wave_id = '",wave_params$wave_id,"'
          AND exposure_tstamp BETWEEN
            TIMESTAMP('",wave_params$start_date,"', '",mojitoReportTimezone,"')
            AND TIMESTAMP('",wave_params$stop_date,"', '",mojitoReportTimezone,"')
          ",segment_clause,"
          AND NOT (
            x.subject IS NULL
            OR x.recipe_name IS NULL
          )
        GROUP BY 1, 2
    )

    select distinct
      exposure_tstamp as exposure_time,
      recipe_name,
      sum(subjects)
      over (
        partition by recipe_name
        order by exposure_tstamp
        rows unbounded PRECEDING ) as subjects,
      sum(conversions)
      over (
        partition by recipe_name
        order by exposure_tstamp
        rows unbounded PRECEDING ) as conversions
    from daily_aggregate
    order BY 1, 2;
    "
  )

  last_query <<- query

  df <- mojitoBqConstrctor(mojito_ds, query)
  df <- as.data.frame(df)
  df$exposure_time <- as.POSIXct(df$exposure_time)

  if ("recipes" %in% names(wave_params)) {
    df <- df[df$recipe_name %in% wave_params$recipes,]
    df$recipe_name <- factor(df$recipe_name, levels = wave_params$recipes)
  }

  last_df <<- df

  return(df)
}





# Traffic conversions segments
mojitoGetUniqueTrafficConversions <- function(wave_params, goal, operand="=", goal_count=1, segment_type=NA, segment_value=NA) {
  
  query <- paste0(
    "
    WITH user_goal_count AS (
    SELECT
      c.subject
    FROM ",wave_params$tables$goal," c
    INNER JOIN ",wave_params$tables$exposure," x
      ON c.subject = x.subject
        and x.wave_id = '",wave_params$wave_id,"'
        and x.exposure_tstamp < c.conversion_tstamp
        and exposure_tstamp BETWEEN
          convert_timezone('",mojitoReportTimezone,"', 'UTC', '",wave_params$start_date,"')
          and Convert_timezone('",mojitoReportTimezone,"', 'UTC', '",wave_params$stop_date,"')
        and conversion_tstamp BETWEEN
          convert_timezone('",mojitoReportTimezone,"', 'UTC', '",wave_params$start_date,"')
          and Convert_timezone('",mojitoReportTimezone,"', 'UTC', '",wave_params$stop_date,"')
        and goal ",operand," '",goal,"'
    GROUP BY 1
    HAVING count(*) >= ",goal_count,"
  ),
  user_traffic_source as (
    SELECT DISTINCT
      x.subject,
      last_value(", segment_value ,") over (PARTITION by domain_userid rows between unbounded preceding and unbounded following) as traffic_source
    FROM ",wave_params$tables$exposure," x
    INNER JOIN ",wave_params$tables$channel," t
      ON t.domain_userid = x.subject
        AND x.wave_id = '",wave_params$wave_id,"'
        AND exposure_tstamp BETWEEN convert_timezone('",mojitoReportTimezone,"', 'UTC', '",wave_params$start_date,"')
          and convert_timezone('",mojitoReportTimezone,"', 'UTC', '",wave_params$stop_date,"')
        AND t.derived_tstamp <= x.exposure_tstamp
  )

  SELECT
    x.recipe_name,
    nvl(t.traffic_source, 'Direct (not available)') as traffic_source,
    count(DISTINCT x.subject) AS subjects,
    count(DISTINCT c.subject) AS conversions
  FROM ",wave_params$tables$exposure," x
  LEFT JOIN user_goal_count c
    ON x.subject = c.subject
      AND exposure_tstamp BETWEEN convert_timezone('",mojitoReportTimezone,"', 'UTC', '",wave_params$start_date,"')
        and convert_timezone('",mojitoReportTimezone,"', 'UTC', '",wave_params$stop_date,"')
      AND wave_id = '",wave_params$wave_id,"'
      AND NOT (x.subject IS NULL OR x.recipe_name IS NULL)
  LEFT JOIN user_traffic_source t
    ON t.subject = x.subject
  WHERE 
    exposure_tstamp BETWEEN convert_timezone('",mojitoReportTimezone,"', 'UTC', '",wave_params$start_date,"')
      and convert_timezone('",mojitoReportTimezone,"', 'UTC', '",wave_params$stop_date,"')
    AND wave_id = '",wave_params$wave_id,"'
    AND NOT (x.subject IS NULL OR x.recipe_name IS NULL)
  GROUP BY 1, 2
  ORDER BY 3 DESC;
  ")

  last_query <<- query

  df <- mojitoBqConstrctor(mojito_ds, query)

  if ("recipes" %in% names(wave_params)) {
    df <- df[df$recipe_name %in% wave_params$recipes,]
    df$recipe_name <- factor(df$recipe_name, levels = wave_params$recipes)
  }

  return(df)
}


# Time diff for conversions
mojitoGetConversionTimeIntervals <- function(wave_params, goal, operand="=", time_grain="minute", max_interval=30) {

  query <- paste0(
    "
    SELECT
      x.recipe_name,
      datediff('",time_grain,"', min(exposure_tstamp), min(conversion_tstamp)) AS time_interval
    FROM ",wave_params$tables$exposure," x
      INNER JOIN ",wave_params$tables$goal," c
        ON (
          x.subject = c.subject
          AND x.exposure_tstamp < c.conversion_tstamp
          AND exposure_tstamp BETWEEN
            Convert_timezone('",mojitoReportTimezone,"', 'UTC', '",wave_params$start_date,"')
            AND Convert_timezone('",mojitoReportTimezone,"', 'UTC', '",wave_params$stop_date,"')
          AND conversion_tstamp BETWEEN
            Convert_timezone('",mojitoReportTimezone,"', 'UTC', '",wave_params$start_date,"')
            AND Convert_timezone('",mojitoReportTimezone,"', 'UTC', '",wave_params$stop_date,"')
          AND goal ",operand," '",goal,"'
        )
    WHERE
      AND wave_id = '",wave_params$wave_id,"'
      AND exposure_tstamp BETWEEN
        Convert_timezone('",mojitoReportTimezone,"', 'UTC', '",wave_params$start_date,"')
        AND Convert_timezone('",mojitoReportTimezone,"', 'UTC', '",wave_params$stop_date,"')
      AND NOT (
        x.subject IS NULL
        OR x.recipe_name IS NULL
      )
    GROUP BY 1, c.subject
    HAVING datediff('",time_grain,"', min(exposure_tstamp), min(conversion_tstamp)) < ",max_interval,";
    "
  )

  df <- mojitoBqConstrctor(mojito_ds, query)

  if ("recipes" %in% names(wave_params)) {
    df <- df[df$recipe_name %in% wave_params$recipes,]
    df$recipe_name <- factor(df$recipe_name, levels = wave_params$recipes)
  }

  return(df)
}



# Revenue data extract
mojitoGetRevenueOrders <- function(wave_params, goal, operand="=", segment=NA, segment_val_operand="=", segment_negative=FALSE) {
  
  if(!is.na(segment)) {
    segment_clause <- paste0("
      AND x.subject ",ifelse(segment_negative, "NOT","")," IN (
        SELECT DISTINCT 
          subject 
        FROM ",wave_params$tables$segment,"
        WHERE segment_type = '",segment$type,"' 
            AND segment_value ",segment_val_operand," '",segment$value,"'
        )")
  } else {
    segment_clause <- ""
  }

  query <- paste0(
    "
    WITH daily_aggregate AS (
        SELECT
          date_trunc('",wave_params$time_grain,"',Convert_timezone('UTC', '",mojitoReportTimezone,"', x.exposure_tstamp)) AS exposure_tstamp,
          x.recipe_name,
          count(DISTINCT x.subject) as subjects,
          count(c.subject) as transactions,
          sum(revenue) as revenue
        FROM ",wave_params$tables$exposure," x
        LEFT JOIN ",wave_params$tables$goal," c
          ON (
            x.subject = c.subject
            AND x.exposure_tstamp < c.conversion_tstamp
            AND exposure_tstamp BETWEEN 
              Convert_timezone('",mojitoReportTimezone,"','UTC','",wave_params$start_date,"')
              AND Convert_timezone('",mojitoReportTimezone,"','UTC','",wave_params$stop_date,"')
            AND conversion_tstamp BETWEEN 
              Convert_timezone('",mojitoReportTimezone,"','UTC','",wave_params$start_date,"')
              AND Convert_timezone('",mojitoReportTimezone,"','UTC','",wave_params$stop_date,"')
            AND revenue IS NOT NULL
            AND goal ",operand," '",goal,"'
          )
        WHERE
          AND wave_id = '",wave_params$wave_id,"'
          AND exposure_tstamp BETWEEN 
            Convert_timezone('",mojitoReportTimezone,"','UTC','",wave_params$start_date,"')
            AND Convert_timezone('",mojitoReportTimezone,"','UTC','",wave_params$stop_date,"')
          AND NOT (
            x.subject IS NULL
            OR x.recipe_name IS NULL
          )
          ",segment_clause,"
        GROUP  BY 1, 2
    )

    select distinct
      exposure_tstamp,
      recipe_name,
      sum(subjects) over (partition by recipe_name order by exposure_tstamp rows unbounded PRECEDING) as subjects,
      sum(transactions) over (partition by recipe_name order by exposure_tstamp rows unbounded PRECEDING) as transactions,
      sum(revenue) over (partition by recipe_name order by exposure_tstamp rows unbounded PRECEDING) as revenue
    from daily_aggregate
    order BY 1,2;
    "
  )

  last_query <<- query

  df <- mojitoBqConstrctor(mojito_ds, query)

  if ("recipes" %in% names(wave_params)) {
    df <- df[df$recipe_name %in% wave_params$recipes,]
  }

  return(df)

}


# Get Errors Chart
# Show errors by component (recipe/trigger) by the specified time grain
mojitoGetErrorsChart <- function(wave_params) {
  query <<- paste0("
    SELECT 
      component,
      DATE_TRUNC('",wave_params$time_grain,"', convert_timezone('UTC', '",mojitoReportTimezone,"', derived_tstamp)) AS tstamp,
      COUNT(*) AS errors,
      count(distinct subject) AS subjects
    FROM ",wave_params$tables$failure,"
    WHERE derived_tstamp >= '",wave_params$start_date,"'
      AND derived_tstamp <= '",wave_params$stop_date,"'
      AND wave_id = '",wave_params$wave_id,"'
    GROUP BY 1, 2
    ORDER BY 3 DESC;
    ")

  tryCatch({
    last_result <<- mojitoBqConstrctor(mojito_ds, query)

    return(last_result)
  }, finally=function(){
    last_result <<- data.frame()
  })
  
}

# Get Errors Table
# Get the top 10 errors for showing in reports
mojitoGetErrorsTab <- function(wave_params) {
  
  query <<- paste0("
    SELECT 
      component,
      error AS error,
      COUNT(*) AS errors,
      count(distinct subject) AS subjects
    FROM ",wave_params$tables$failure,"
    WHERE derived_tstamp >= '",wave_params$start_date,"'
      AND derived_tstamp <= '",wave_params$stop_date,"'
      AND wave_id = '",wave_params$wave_id,"'
    GROUP BY component, error
    ORDER BY 3 DESC
    LIMIT 10;
    ")

  last_result <<- mojitoBqConstrctor(mojito_ds, query)

  return(last_result)
}
