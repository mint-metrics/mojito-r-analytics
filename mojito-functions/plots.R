#
# Mojito Plot functions
#

# Plot unique conversion rate delta
mojitoPlotUniqueDelta <- function(wave_params, dailyDf) {

  dailyDf$cvr <- dailyDf$conversions/dailyDf$subjects
  long <- melt(dailyDf,id.vars = c("exposure_time", "recipe_name"), measure.vars = c("subjects", "conversions", "cvr"))
  data <- cast(long, exposure_time ~ recipe_name + variable)

  if ("recipes" %in% names(wave_params)) {
    for (i in wave_params$recipes) {
      control <- wave_params$recipes[1]
      # CVR deltas
      if (wave_params$recipes[1]!=i) {
        data[,paste0(i,"_delta")] <- (data[,paste0(i,"_cvr")]-data[,4])/data[,4]
      
        # P values
        for (n in 1:length(data[,1])) {
          badCvr <- !(data[n,4]>0 && data[n,4]<1)
          badValues <- any(is.na(data[n,4:6])) | 0 %in% data[n,4:6] | is.na(data[n,paste0(i,"_conversions")]) | is.na(data[n,paste0(control,"_conversions")])
          
          data[n,paste0(i,"_pv")] <- ifelse(
            badCvr | badValues,
            NA, 
            prop.test(
              x=c(
                data[n,paste0(i,"_conversions")], 
                data[n,paste0(control,"_conversions")]
              ), 
              n=c(
                data[n,paste0(i,"_subjects")],
                data[n,paste0(control,"_subjects")]
              )
            )$p.value
          )

        }
      }
    }
  }

  recipe_count <- length(wave_params$recipes)
  rows_count <- length(data[,1])
  recipe_name_vector <- delta_vector <- pvalue_vector <- c()
  for (i in 1:(recipe_count-1)) {
    recipe_name_vector <- c(recipe_name_vector,rep(wave_params$recipes[i+1],rows_count))
    delta_vector <- c(delta_vector, c(data[,((8+(3*(recipe_count-2)))+((i-1)*2))]))
    pvalue_vector <- c(pvalue_vector, c(data[,((9+(3*(recipe_count-2)))+((i-1)*2))]))
  }
  plot_data <- data.frame(exposure_time=rep(data[,1],(recipe_count-1)),recipe_name=recipe_name_vector,delta=delta_vector,pvalue=pvalue_vector)

  delta <- ggplot(plot_data) + 
    geom_hline(aes(yintercept=0), linetype="dashed") +
    xlab(NULL) + ylab("% lift") + scale_y_continuous(labels=percent) +
    geom_line(aes(x=exposure_time, y=delta,colour=recipe_name)) + 
    theme(legend.position="none")
  
  pvalue <- ggplot(plot_data) + 
    geom_hline(aes(yintercept=0.05), colour="#50C878") + 
    xlab(NULL) + ylab("p-value") + scale_y_reverse() +
    geom_line(aes(x=exposure_time, y=pvalue,colour=recipe_name)) +
    theme(legend.position="bottom") + scale_color_discrete(name=NULL)
  
  print(delta)
  print(pvalue)

}


# Conversion time intervals plot & table
mojitoTimeIntervals <- function(wave_params, goal,time_grain="minutes",max_interval=30) {
  data <- mojitoGetConversionTimeIntervals(
    wave_params, 
    goal,
    time_grain, 
    max_interval)

  binnum <- ifelse(max_interval>=30,30,max_interval-1)

  plot <- ggplot(data, aes(x=time_interval, fill=recipe_name, alpha=0.1)) + 
    geom_area(aes(),stat="bin", position="identity", bins=binnum) +
    ylab("Subjects") + xlab(paste0(tools::toTitleCase(time_grain), " to convert (<",max_interval,")")) +
    scale_alpha(guide=F) + theme(legend.position = "bottom", legend.title = element_blank())

  print(plot)

  tab <- data %>%
    group_by(recipe_name) %>%
    summarise(Q1 = quantile(time_interval, 0.25),
              Median = median(time_interval),
              Q3 = quantile(time_interval, 0.75),
              Avg = mean(time_interval),
              Min = min(time_interval),
              Max = max(time_interval))

  tab <- data.frame(tab[,-1], row.names = gsub("(#|-|_)", " ", levels(tab$recipe_name)))

  print(ztable(tab, size=7))
}
