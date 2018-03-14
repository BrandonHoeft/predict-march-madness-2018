# Using the work made available by Zach Mayer at https://github.com/zachmayer/kaggleNCAA

# Simulate Bracket, given my inputs: head-to-head  predicted probabilities.
# parseBracket points to a local file system. Doesn't work with functions from Aws.s3. 
library(kaggleNCAA)
set.seed(321)
dat <- parseBracket(f = "~/Downloads/rf_preds_2018_submission1.csv", w = 0)  # w=0 for men
sim <- simTourney(dat, 10000, progress=TRUE)  
bracket <- extractBracket(sim)  
printableBracket(bracket) 

png(filename="rf_10000_sims.png", width = 1000, height = 600, pointsize = 18)
printableBracket(bracket)
dev.off()

printableBracket(bracket) 

# sim about a season's worth of games. 
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
