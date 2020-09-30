#
# Mojito reports functions
#

library("ggplot2")
library("scales")
library("reshape")
library("ztable")
library("dplyr")
library("stats")
library("knitr")
library("jsonlite")

# Report options
# Requires PNG Quant to be installed - https://pngquant.org/
knit_hooks$set(pngquant = hook_pngquant)
options(ztable.type="html")
if ((is.na(Sys.timezone()) || is.null(Sys.timezone())) && !exists("mojitoReportTimezone")) {
  stop("Mojito Reports: Set your R timezone or supply an override e.g. 'Australia/Melbourne' via the `mojitoReportTimezone` variable")
} else {
  mojitoReportTimezone <- Sys.timezone()
}



# Date formatting at the top of the report
mojitoReportDates <- function(wave_params) {
  ifelse(as.POSIXct(wave_params$stop_date,mojitoReportTimezone)<Sys.time(),
    return(
      paste0(
        wave_params$stop_date,
        " (Ran for ",floor(difftime(as.POSIXct(wave_params$stop_date,mojitoReportTimezone), 
        as.POSIXct(wave_params$start_date,mojitoReportTimezone), units = "days")),
        " days)"
        )
      ),
    return(
      paste0(
        "Still active (Running for ",
        floor(difftime(Sys.time(), as.POSIXct(wave_params$start_date,mojitoReportTimezone), units = "days")),
        " days)"
        )
      )
    )
}


# Handle ETA estimates formatting
mojitoReportExperimentSizing <- function(conversion_point) {
  if (exists("wave_eta")) {
    return(
      paste0(
        round(wave_eta$estimate_data$days_to_run, 1), " days",
        " w/ ", percent(wave_eta$delta), " MDE in ", conversion_point,
        " and ~", comma(round(wave_eta$estimate_data$subjects/wave_eta$recipes, 0)), " subjects/recipe."
      )
    )
  }
}

# Diagnotics plot & SRM test
# Useful for diagnosing bad assignment ratios and tracking issues
mojitoDiagnostics <- function(wave_params, dailyDf, proportions = NULL) {

  # Plot exposed users per time grain
  exposed_users <- dailyDf %>%
    group_by(recipe_name) %>%
    mutate(exposed = c(subjects[1], diff(subjects)))

  exposed_plot <- ggplot(exposed_users, aes(exposure_time, exposed, color=recipe_name)) +
    geom_line(stat="identity") + scale_y_log10() +
    ylab("Subjects") + xlab("Exposure time") +
    scale_color_discrete(guide=F)

  print(exposed_plot)


  # Plot ratios over time
  srmplot_data <- dailyDf %>%
    group_by(exposure_time) %>%
    mutate(total_subjects = sum(subjects, na.rm=T)) %>%
    ungroup()
  
  srmplot_data$ratio <- srmplot_data$subjects / srmplot_data$total_subjects
  
  srm_plot <- ggplot(srmplot_data) + 
    geom_line(aes(exposure_time, ratio, color=recipe_name), stat="identity") +
    ylab("Ratio") + xlab("Exposure time") +
    scale_alpha(guide=F) + theme(legend.position = "bottom", legend.title = element_blank())

  print(srm_plot)


  # SRM test
  # Using chisq.test() as per @lukasvermeer's test https://github.com/lukasvermeer/srm/blob/a5c529c2f816b98730dd5e337b711cc022ded7e0/chrome-extension/tests/computeSRM.test.js
  if (is.null(proportions)) {
    proportions <- rep(1/length(wave_params$recipes), length(wave_params$recipes))
  }
  df <- dailyDf %>%
    filter(recipe_name %in% wave_params$recipes) %>%
    group_by(recipe_name) %>%
    transmute(subjects = max(subjects)) %>%
    distinct(.keep_all = F)
  srm_test <- chisq.test(x = df$subjects, p = proportions)

  srm_message <- ""
  for (srm_i in 1:length(wave_params$recipes)) {
    expected <- proportions[srm_i]
    observed <- df$subjects[srm_i]/sum(df$subjects)
    srm_message <- paste0(srm_message, "<br />", wave_params$recipes[srm_i], ": ", percent(observed, ), " (", percent(expected), " expected)")
  }

  cat(paste0("SRM p-value: ", pvalue(srm_test$p.value), srm_message))


  # Plot and output errors if available
  if (!is.null(wave_params$tables$failure)) {
    error_plot_data <- mojitoGetErrorsChart(wave_params)
    if (length(error_plot_data) == 4) {
      if (length(error_plot_data$tstamp) != 0) {
        cat(paste0("<br /><h3>Errors tracked</h3>"))
        error_plot <- ggplot(error_plot_data, aes(tstamp, color=component)) +
          geom_line(aes(y=subjects)) +
          xlab(NULL) + ylab("Errors") + theme(legend.position="bottom")
        print(error_plot)

        tab_data <- mojitoGetErrorsTab(wave_params)
        colnames(tab_data) <- c("Component", "Error message", "Total errors", "Subjects")
        knitr::kable(tab_data, format = "html")
      }
    }
  }

}


# Unique conversions report
# Plot p-value / delta charts and a summary table for a given metric/segment combination
mojitoUniqueConversions <- function(wave_params, goal, goal_count=1, segment=NA, segment_val_operand="=", segment_negative=TRUE) {

  tryCatch({
    result <- mojitoGetUniqueConversions(wave_params, goal, goal_count, segment, segment_val_operand, segment_negative)
    last_result <<- result
  }, error = function(e){
    print(paste("Error: ",e))
  })

  # Populate recipe names for convenience
  if (!("recipes" %in% names(wave_params))) {
    wave_params$recipes <- unique(result$recipe_name)
  }

  # Run plots & table
  if (exists("result")) {
    tryCatch({
      mojitoPlotUniqueDelta(wave_params, result)
      mojitoTabUniqueCvr(wave_params, result)
    }, error = function(e){
      print(paste("Error - check your data.",e))
      print(result)
      print(wave_params)
    })  
  }
}



# Create the full knit from the goal_list
# Pass in a list of goals to iterate through
mojitoFullKnit <- function(wave_params, goal_list=NA) {
  for (i in 1:length(lapply(goal_list, "["))) {

    itemList <- lapply(goal_list, "[")[[i]]

    # TODO: Roll up Revenue & Time to convert & Diagnostics reports into full knitter
    
    if (!is.null(itemList$segment_type) && itemList$segment_type == "traffic") {
      # Traffic segments data
      goal_list[[i]]$result <- mojitoGetUniqueTrafficConversions(
        wave_params=wave_params, 
        goal=itemList$goal, 
        goal_count=ifelse(is.null(itemList$goal_count),1,itemList$goal_count),
        segment_type=ifelse(is.null(itemList$segment_type),NA,itemList$segment_type),
        segment_value=ifelse(is.null(itemList$segment_value),NA,itemList$segment_value)
      )
    } else {
      # Standard conversions data
      goal_list[[i]]$result <- mojitoGetUniqueConversions(
        wave_params=wave_params, 
        goal=itemList$goal, 
        goal_count=ifelse(is.null(itemList$goal_count),1,itemList$goal_count),
        segment_type=ifelse(is.null(itemList$segment_type),NA,itemList$segment_type),
        segment_value=ifelse(is.null(itemList$segment_value),NA,itemList$segment_value), 
        segment_val_operand=ifelse(is.null(itemList$segment_val_operand), "=", itemList$segment_val_operand), 
        segment_negative=ifelse(is.null(itemList$segment_negative), F, itemList$segment_negative)
      )

      if (!("recipes" %in% names(wave_params))) {
        wave_params$recipes <- unique(result$recipe_name)
      }

      rowResult <- mojitoSummaryTableRows(goal_list[[i]]$result, wave_params = wave_params, goal_list=goal_list[[i]])

      if (exists("summaryDf")) {
        summaryDf <- rbind(summaryDf, rowResult)
      } else {
        summaryDf <- rowResult
      }

    }
  
  }
  
  # Summary table print
  if (exists("summaryDf")) mojitoTabulateSummaryDf(summaryDf)
  
  # Print sections of the report
  for (i in 1:length(lapply(goal_list, "["))) {
    cat(paste0("<br /><h2>", goal_list[[i]]$title, "</h2>"))
    if (!is.null(goal_list[[i]]$segment_type) && goal_list[[i]]$segment_type == "traffic") {
      # Traffic segments table
      mojitoTabUniqueTrafficCvr(wave_params, goal_list[[i]]$result)
    } else {
      # Standard unique conversions plot
      mojitoPlotUniqueDelta(wave_params, goal_list[[i]]$result)
      cat("<br />")
      mojitoTabUniqueCvr(wave_params, goal_list[[i]]$result)
    }
  }

  # Save data to HTML & return full list from function
  cat(paste0("<meta name='mojitoReportDataBase64' id='mojitoReportData' value='",
    jsonlite::base64_enc(jsonlite::toJSON(goal_list, "rows")),
    "'>"))
  return(goal_list)
  
}

