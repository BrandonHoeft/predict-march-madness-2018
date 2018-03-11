library(readr)
library(dplyr)
massey_data <- read_csv(file = "~/Downloads/PrelimData2018/MasseyOrdinals_Prelim2018.csv")

# Analyze which ratings have been published back to 2003. 
ratings_since_2003 <- massey_data %>%
    group_by(SystemName) %>%
    summarize(beginning_yr = min(Season),
              last_yr = max(Season),
              total_ratings = n()) %>%
    ungroup() %>%
    filter(beginning_yr == 2003) %>%
    arrange(beginning_yr)

# get Sagarin, Pomeroy, and RPI ratings data for games back to 2003 - 2018.
sag_pom_rpi <- massey_data %>%
    filter(SystemName %in% c("SAG", "POM", "RPI"))

# write to flat file, which was then uploaded to an AWS S3 bucket.
write_csv(sag_pom_rpi,
          path = "~/Downloads/PrelimData2018/MasseyOrdinals_sag_pom_rpi_Prelim2018.csv")
