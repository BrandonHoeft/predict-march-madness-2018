# Using the work made available by Zach Mayer at https://github.com/zachmayer/kaggleNCAA

# Simulate Bracket, given my inputs: head-to-head  predicted probabilities.
# parseBracket points to a local file system. Doesn't work with functions from Aws.s3. 
library(kaggleNCAA)


#########################################################################################
# Simulate bracket Using  RF_fit_2003_2018 (random forest model)
#########################################################################################

# 10,000 simulations  ###################################################################

set.seed(321)
dat <- parseBracket(f = "~/Downloads/rf_preds_2018_submission1.csv", w = 0)  # w=0 for men
sim <- simTourney(dat, 10000, progress=TRUE)  
bracket <- extractBracket(sim)  
printableBracket(bracket) 

png(filename="rf_10000_sims.png", width = 1000, height = 600, pointsize = 18)
printableBracket(bracket)
dev.off()

printableBracket(bracket) 

# sim about a season's worth of games. ###################################################
set.seed(321)
sim_short <- simTourney(dat, 30, progress=TRUE) 
bracket2 <- extractBracket(sim_short)  
printableBracket(bracket2) 

png(filename="rf_30_sims.png", width = 1000, height = 600, pointsize = 18)
printableBracket(bracket2)
dev.off()

# Walk through the bracket. assume my pred probabilities are transitive
walk_bracket <- walkTourney(dat)
printableBracket(walk_bracket)
png(filename="rf_walk_bracket.png", width = 1000, height = 600, pointsize = 18)
printableBracket(walk_bracket)
dev.off()


#########################################################################################
# Simulate bracket Using  LASSO_fit_2003_2018
#########################################################################################

# 10,000 simulations
set.seed(1988)
lasso_dat <- parseBracket(f = "~/Downloads/LASSO_preds_2018_submission1.csv", w = 0)  # w=0 for men
LASSO_sim <- simTourney(lasso_dat, 10000, progress=TRUE)  
LASSO_bracket <- extractBracket(LASSO_sim) 
printableBracket(LASSO_bracket)

png(filename="LASSO_10000_sims.png", width = 1000, height = 600, pointsize = 18)
printableBracket(LASSO_bracket)
dev.off()

# write the underlying data from 10,000 simulations of LASSO predictions to AWS S3
# can compare to bracket for learning the probability distributions. 
LASSO_bracket %>%
    inner_join(team_name, by = c("winner" = "TeamID")) %>%
    select(-ends_with("D1Season"), -women) %>%
    select(TeamName, winner, everything()) %>%
    arrange(TeamName, slot) %>% 
    s3write_using(
                  FUN = write.csv,
                  bucket = "ncaabasketball",
                  object = "LASSO_10000_simulation_data.csv")


# sim just about a season's worth of games.  #############################################
set.seed(54321)
LASSO_sim_short <- simTourney(lasso_dat, 30, progress=TRUE) 
LASSO_bracket2 <- extractBracket(LASSO_sim_short)  
printableBracket(LASSO_bracket2) 

png(filename="LASSO_30_sims.png", width = 1000, height = 600, pointsize = 18)
printableBracket(LASSO_bracket2)
dev.off()
