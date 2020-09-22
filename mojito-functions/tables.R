#
# Mojito Table functions
#

# Unique conversion table
mojitoTabUniqueCvr <- function(wave_params, dailyDf) {
  recipes <- length(unique(dailyDf$recipe_name))

  # Get the last two fields in the dailyDf dataframe and order results if recipes are specified
  expResult <- dailyDf %>%
    group_by(recipe_name) %>%
    transmute(subjects = max(subjects,na.rm = T),
              conversions = max(conversions,na.rm = T)) %>%
    distinct(.keep_all = T) %>%
    data.frame()

  if ("recipes" %in% names(wave_params)) {
    expResult <- expResult[order(expResult$recipe_name),]
  }

  expResult$cvr <- expResult$conversions / expResult$subjects
  expResult$p <- expResult$lift <- NA
  
  for (i in 2:length(expResult$conversions)) {
    tempLift <- ((expResult$cvr[i]-expResult$cvr[1])/expResult$cvr[1])
    expResult$lift[i] <- ifelse(!is.nan(tempLift) && is.numeric(tempLift), percent(tempLift), NA)
    expResult$p[i] <- ifelse(
        expResult$conversions[1] == 0 | is.null(expResult$conversions[1]) | expResult$conversions[i] == 0 | is.null(expResult$conversions[i]),
        1, 
        (
          prop.test(
            x=c(expResult$conversions[1],expResult$conversions[i]),
            n=c(expResult$subjects[1],expResult$subjects[i])
          )$p.value
        )
    )
  }

  expResult$cvr <- percent(expResult$cvr)
  expResult$p[2:length(expResult$p)] <- pvalue(expResult$p[2:length(expResult$p)])
  expResult$subjects <- comma(expResult$subjects)
  expResult$conversions <- comma(expResult$conversions)
  colnames(expResult) <- c("Recipe","Subjects","Goals","\\% Conv.", "\\% Lift", "p-Value")
  backup <- expResult
  expResult <- expResult[,-1]
  expResult[1,c(4,5)] <- ""
  rownames(expResult) <- gsub("(#|-|_|&)", " ", backup[,1])

  tab <- expResult
  tab <- ztable(tab, size=7) #%>%
    #makeHeatmap(palette="RdYlGn", cols=c(3)) # heatmap for lifts column - requires ztable 0.2.0 
  for (i in 2:recipes) {
    if (backup[i,6]<0.05 && !is.na(backup[i,5])) {
        tab=addCellColor(tab, rows=c(i+1), cols=c(6), "mediumspringgreen")
        if (backup[i,5]>0) {
            tab=addCellColor(tab, rows=c(i+1), cols=c(5), "mediumspringgreen")
        } else {
            tab=addCellColor(tab, rows=c(i+1), cols=c(5), "lightcoral")
        }
    }
  }
  
  print(tab)

}



# Traffic segments table
mojitoTabUniqueTrafficCvr <- function(wave_params, df) {
  result <- df
  result$ratio <- NA
  result$lift <- NA

  for (i in 1:length(result$traffic_source)) {
    controlRecord <- result[result$recipe_name == wave_params$recipes[1] & result$traffic_source == result$traffic_source[i],]
    if (length(controlRecord$subjects) < 1) {
      result$ratio[i] <- NA
      result$lift[i] <- NA
    } else {
      controlCvr <- (controlRecord$conversions / controlRecord$subjects) 
      result$ratio[i] <- tryCatch({
            percent(result$subjects[i] / (result$subjects[i] + controlRecord$subjects))
        }, finally = {
            result$subjects[i] / (result$subjects[i] + controlRecord$subjects)
        })
      if (controlCvr != 0) {
        result$lift[i] <- tryCatch({
              percent((controlCvr - (result$conversions[i] / result$subjects[i])) / controlCvr)
          }, finally = {
              ((controlCvr - (result$conversions[i] / result$subjects[i])) / controlCvr)
          })
      }
    }
  }
  result <- result[result$recipe_name != wave_params$recipes[1], c(1,2,3,5,6)]
  result$subjects <- comma(result$subjects)
  colnames(result) <- c("Recipe", "Source", "Subjects", "Ratio", "% lift")
  tab <- ztable(result, size=7, align="llccc")
  print(tab)
}



# Tabulate revenue data
mojitoTabRevenue <- function(dailyDf) {

  # TODO: Remove exposure tstamp window function from Redshift
  if ("exposure_tstamp" %in% colnames(dailyDf)) {
    tab <- dailyDf %>%
      group_by(recipe_name) %>%
      transmute(subjects = max(subjects,na.rm = T),
                transactions = max(transactions,na.rm = T),
                revenue = max(revenue,na.rm = T)) %>%
      distinct(.keep_all = T) %>%
      data.frame()
  } else {
    tab <- dailyDf
  }

  tab$trans_per_subject <- tab$transactions/tab$subjects
  tab$revenue_per_trans <- tab$revenue/tab$transactions
  tab$revenue_per_subject <- tab$revenue/tab$subjects

  tab$subjects <- comma(tab$subjects,digits = 0)
  tab$transactions <- comma(tab$transactions,digits = 0)
  tab$revenue <- paste0("\\",dollar(tab$revenue))
  tab$revenue_per_trans <- paste0("\\",dollar(tab$revenue_per_trans))
  tab$trans_per_subject <- round(tab$trans_per_subject,digits = 3)
  tab$revenue_per_subject <- paste0("\\",dollar(tab$revenue_per_subject))
  
  rownames(tab) <- gsub("(#|-|_|&)", " ", tab[,1])
  tab <- tab[,-1]
  colnames(tab) <- c("Subjects", "Transactions", "Revenue", "Txns/subject", "\\$/Txn", "\\$/subject")

  print(ztable(tab, size=7))

}


# Create Summary table rows from goal_list
mojitoSummaryTableRows <- function(dailyDf, wave_params, goal_list) {
  recipeCnt <- length(unique(dailyDf$recipe_name))

  # Get the last two fields in the dailyDf dataframe and order results if recipes are specified
  rowResult <- dailyDf %>%
    group_by(recipe_name) %>%
    transmute(subjects = max(subjects,na.rm = T),
              conversions = max(conversions,na.rm = T)) %>%
    distinct(.keep_all = T) %>%
    data.frame()

  if ("recipes" %in% names(wave_params)) {
    rowResult <- rowResult[order(rowResult$recipe_name),]
  }

  rowResult$cvr <- rowResult$conversions / rowResult$subjects
  rowResult$goal <- goal_list$title
  rowResult$p <- rowResult$lift <- NA
  
  for (i in 2:length(rowResult$conversions)) {
    tempLift <- ((rowResult$cvr[i]-rowResult$cvr[1])/rowResult$cvr[1])
    rowResult$lift[i] <- ifelse(is.numeric(tempLift) && !is.nan(tempLift), percent(tempLift), NA)
    rowResult$p[i] <- ifelse(
        rowResult$conversions[1] == 0 | is.null(rowResult$conversions[1]),
        0, 
        (prop.test(
          x=c(rowResult$conversions[1],rowResult$conversions[i]),
          n=c(rowResult$subjects[1],rowResult$subjects[i]))$p.value)
    )
  }

  rowResult$cvr <- percent(rowResult$cvr)
  rowResult$p[2:length(rowResult$p)] <- pvalue(rowResult$p[2:length(rowResult$p)])
  rowResult$subjects <- comma(rowResult$subjects)
  rowResult$conversions <- comma(rowResult$conversions)
  rowResult <- rowResult[,c(5,1,6,7)]
  rowResult <- rowResult[!is.na(rowResult$p),]
  rowResult[,2] <- gsub("(#|-|_|&)", " ", rowResult[,2])

  return(rowResult)
}


# Create Summary table
mojitoTabulateSummaryDf <- function(summaryDf) {
  colnames(summaryDf) <- c("Goal", "Recipe", "% lift", "p-value")

  # Hide treatment names if only 2 variants
  recipeCnt <- length(unique(summaryDf$Recipe))
  columnOffset <- 0
  if (recipeCnt == 1) {
    summaryDf <- summaryDf[,c(1,3,4)]
    columnOffset = columnOffset + 1
  }
  pValueCol <- 4-columnOffset
  liftCol <- 3-columnOffset

  # Highlight statistically significant values
  tab <- ztable(summaryDf, size=7, align="llcc", include.rownames=FALSE)
  for (i in 1:length(summaryDf$Goal)) {
    if (summaryDf[i,pValueCol]<0.05 && !is.na(summaryDf[i,liftCol])) {
      tab <- addCellColor(tab, rows=c(i+1), cols=c(pValueCol+1), c("mediumspringgreen"))
      if (summaryDf[i,liftCol]>0) {
        tab <- addCellColor(tab, rows=c(i+1), cols=c(liftCol+1), c("mediumspringgreen"))
      } else {
        tab <- addCellColor(tab, rows=c(i+1), cols=c(liftCol+1), c("lightcoral"))
      }
    }
  }
  print(tab)
}
