library(aws.s3)
library(readr)
library(dplyr)
# specify personal account keys as environment variables so I can read my s3 object(s) from AWS. 
# DO NOT SAVE KEY in code or render in output!!!! Could compromise AWS account. 
# Note to self my key and secret last gen from October 2017. 
#Sys.setenv("AWS_ACCESS_KEY_ID" = "",
#           "AWS_SECRET_ACCESS_KEY" = "")

ken_pom_2018 <- s3read_using(FUN = read_csv, 
                             object = "KenPomSummary2018.csv",
                             bucket = "ncaabasketball") %>%
    arrange(desc(AdjEM))


south_bracket <- ken_pom_2018 %>%
    filter(TeamName %in% c("Miami FL", "Loyola Chicago", "Tennessee", "Wright St.",
                           "Nevada", "Texas", "Cincinnati", "Georgia St.")) %>% 
    select(TeamName, AdjEM, AdjTempo) 


simulate_win_prob <- function(teamA_AdjEM, teamA_adjTempo, teamB_AdjEM, teamB_adjTempo) {
    teamA_score_diff <- (teamA_AdjEM - teamB_AdjEM) * ((teamA_adjTempo + teamB_adjTempo) / 68.4)
    # 1 minus CDF of a team score diff of +1 points or less. 
    teamA_win_prob <- 1 - pnorm(1, mean = teamA_score_diff, sd = 11)
    mean(runif(1000, 0, 1) < teamA_win_prob)
}

simulate_win_prob(south_bracket[[6,2]], south_bracket[[6,3]], south_bracket[[4,2]], south_bracket[[4,3]])

