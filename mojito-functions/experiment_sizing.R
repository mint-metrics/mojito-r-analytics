#
# Mojito estimated run time functions
#

# Experiment duration calculator
# Given an experiment trigger, conversion point an
experimentTargetingQuery <- function(trigger_clause, conversion_clause, app_id, subject="domain_userid") {
  last_query <<- paste0("
    WITH subjects as (
    		SELECT DISTINCT
      ",subject," as subject, min(derived_tstamp) as exposure_tstamp
      FROM ",app_id,".events
      WHERE
      collector_tstamp between '",as.Date(Sys.time())-31,"' and '",as.Date(Sys.time())-1,"'
      and ",trigger_clause,"
      GROUP BY 1
      ),
      
      conversions as (
      SELECT DISTINCT
      ",subject," as subject, min(derived_tstamp) as conversion_tstamp
      FROM ",app_id,".events
      WHERE
      collector_tstamp between '",as.Date(Sys.time())-31,"' and '",as.Date(Sys.time())-1,"'
      and ",conversion_clause,"
      GROUP BY 1
      )
      
    select count(s.subject) as subjects, count(c.subject) as conversions
    from subjects s
    left join conversions c
    on s.subject = c.subject and c.conversion_tstamp > s.exposure_tstamp
    ")
  
  return(dbGetQuery(con, last_query))
}

# Estimate days to run for significance
# Given traffic & conversion estimates along with delta/treatments/power, return the days to run for a statistically significant result
durationEstimation = function(users, conversions, delta, recipes, stat_power)
{
  p1 <- (conversions/users)
  p2 <- (p1*delta)+p1
  
  estimate <- power.prop.test(n=NULL, p1=p1, p2=p2, sig.level = 0.05, power = stat_power, alternative = "two.sided")
  
  # recipes expects control as one of the treatments - i.e. straight A/B test will count as 2 recipes
  return((estimate$n*recipes)/(users/30))
}

# Experiment sizing calculator main function
# Provide an estimate given an experiment's parameters for querying against actual event data
estimateDurationQuery <- function(app_id, trigger_clause, conversion_clause, subject="domain_userid", delta, recipes, stat_power) {
  estimateData <- experimentTargetingQuery(trigger_clause, conversion_clause, app_id, subject)
  estimateData$base_cvr <- estimateData$conversions / estimateData$subjects
  estimateData$target_cvr <- estimateData$base_cvr*(1+delta)
  days_to_run <- durationEstimation(users=estimateData$subjects, conversions=estimateData$conversions, delta, recipes, stat_power)
  print(paste("Days to run:", days_to_run))
  print(estimateData)

  # Return a list with the estimate results
  estimateData$days_to_run <- days_to_run
  eta_list <- list(
      "app_id" = app_id,
      "trigger_clause" = trigger_clause,
      "conversion_clause" = conversion_clause,
      "subject" = subject,
      "delta" = delta,
      "recipes" = recipes,
      "stat_power" = stat_power,
      "eta_timestamp" = Sys.time(),
      "estimate_data" = estimateData
  )
  return(eta_list)
}


# Experiment sizing calculator main function
# Provide an estimate given an experiment's expected traffic & conversion volume
estimateDurationRaw <- function(subjects, conversions, delta, recipes, stat_power) {
  estimateData <- data.frame(subjects=subjects, conversions=conversions)
  estimateData$base_cvr <- estimateData$conversions / estimateData$subjects
  estimateData$target_cvr <- estimateData$base_cvr*(1+delta)
  days_to_run <- durationEstimation(users=estimateData$subjects, conversions=estimateData$conversions, delta, recipes, stat_power)
  print(paste("Days to run:", days_to_run))

  # Return a list with the estimate results
  estimateData$days_to_run <- days_to_run
  eta_list <- list(
      "delta" = delta,
      "recipes" = recipes,
      "stat_power" = stat_power,
      "estimate_data" = estimateData
  )
  return(eta_list)

}
