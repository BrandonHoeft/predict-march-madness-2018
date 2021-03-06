#########################################################################################
# Script for scoring the 2018 NCAA Tourney participant games for the Google-Kaggle Challenge.
#########################################################################################


#########################################################################################
# Data Pre-processing
#########################################################################################
library(aws.s3)
library(readr)
library(dplyr)
# specify personal account keys as environment variables so I can read my s3 object(s) from AWS. 
# DO NOT SAVE KEY in code or render in output!!!! Could compromise AWS account. 
# Note to self my key and secret last gen from October 2017. 
#Sys.setenv("AWS_ACCESS_KEY_ID" = "",
#           "AWS_SECRET_ACCESS_KEY" = "")

RegularSeasonDetailedResults_Prelim2018 <- s3read_using(FUN = read_csv, 
                                                        object = "RegularSeasonDetailedResults_Prelim2018.csv",
                                                        bucket = "ncaabasketball") %>%
    mutate(tournament_type = "regular season") %>%
    select(tournament_type, everything())


team_conference <- s3read_using(FUN = read_csv, 
                                object = "TeamConferences.csv",
                                bucket = "ncaabasketball")

team_name <- s3read_using(FUN = read_csv, 
                          object = "Teams.csv",
                          bucket = "ncaabasketball")

# detailed box score results data for all NCAA Tournament games 2003 - 2017
NCAATourneyDetailedResults_Pre2018 <- s3read_using(FUN = read_csv, 
                                                   object = "NCAATourneyDetailedResults.csv",
                                                   bucket = "ncaabasketball") %>%
    mutate(tournament_type = "NCAA Tournament") %>%
    select(tournament_type, everything())

# detailed box score results data for all regular season NCAA D1 games 2003 - 2017
reg_season_details <- RegularSeasonDetailedResults_Prelim2018 %>%
    bind_rows(NCAATourneyDetailedResults_Pre2018) %>%
    # conference of winning team.
    left_join(team_conference, by = c("WTeamID" = "TeamID",
                                      "Season" = "Season")) %>%
    # name of winning team.
    left_join(team_name, by = c("WTeamID" = "TeamID")) %>%
    select(-contains("D1Season")) %>% # remove other cols from team_name
    rename(WTeam_conf = ConfAbbrev,
           WTeam_name = TeamName) %>%
    # conference of losing team.
    left_join(team_conference, by = c("LTeamID" = "TeamID",
                                      "Season" = "Season")) %>%
    # name of losing team.
    left_join(team_name, by = c("LTeamID" = "TeamID")) %>%
    select(-contains("D1Season")) %>%
    rename(LTeam_conf = ConfAbbrev,
           LTeam_name = TeamName) %>%
    select(tournament_type, Season, DayNum, WTeamID, WTeam_name, WTeam_conf, WScore,
           LTeamID, LTeam_name, LTeam_conf, LScore, everything())


# Bring in the team ratings data
team_ratings <- s3read_using(FUN = read_csv, 
                             object = "MasseyOrdinals_sag_pom_rpi_Prelim2018.csv",
                             bucket = "ncaabasketball") %>%
    tidyr::spread(SystemName, OrdinalRank)

team_season_day_grid <- expand.grid(Season = 2003:2018,
                                    RankingDayNum = 1:154, # 154 is the last day of NCAA tourney. 
                                    TeamID = unique(team_ratings$TeamID)) %>%
    arrange(TeamID, Season, RankingDayNum)

team_ratings_all_days <- team_season_day_grid %>%
    left_join(team_ratings, by = c("Season", "RankingDayNum", "TeamID")) %>%
    select(Season, RankingDayNum, TeamID, POM, RPI, SAG) %>%
    group_by(TeamID, Season) %>%
    arrange(TeamID, Season, RankingDayNum) %>% 
    # http://www.markhneedham.com/blog/2015/06/28/r-dplyr-update-rows-with-earlierprevious-rows-values/
    do(zoo::na.locf(.))




# combine the ratings data with the regular season game data. ###########################
reg_season_details <- reg_season_details %>%
    # rankings of the WINNING team heading into each game day.
    left_join(team_ratings_all_days, by = c("WTeamID" = "TeamID",
                                            "Season" = "Season",
                                            "DayNum" = "RankingDayNum")) %>%
    rename(WPOM_rank = POM,
           WRPI_rank = RPI,
           WSAG_rank = SAG) %>%
    # rankings of the LOSING team heading into each game day.
    left_join(team_ratings_all_days, by = c("LTeamID" = "TeamID",
                                            "Season" = "Season",
                                            "DayNum" = "RankingDayNum")) %>%
    rename(LPOM_rank = POM,
           LRPI_rank = RPI,
           LSAG_rank = SAG)


reg_season_details <- reg_season_details %>%
    mutate(game_id = paste(Season, DayNum, WTeamID, LTeamID, sep = "_")) %>%
    select(game_id, everything())


# Create game features ###################################################################

# get opponent stats so we can calculate defensive effective FG% (FGM, FGM3, FGA), defensive TO%, defensive ORB%, defensive FT rate.
reg_season_details <- reg_season_details %>%
    mutate(Wpossessions = round(0.96 * ((WFGA) - (WOR) + (WTO) + (0.44 * WFTA))), # https://www.nbastuffer.com/analytics101/possession/
           Lpossessions = round(0.96 * ((LFGA) - (LOR) + (LTO) + (0.44 * LFTA))),
           average_tempo = (Wpossessions + Lpossessions) / 2,
           ########## WINNING TEAM's derived statistics ########## 
           # points scored per 100 possessions on offense by winning team. 
           Woffense_efficiency = 100 * (WScore / Wpossessions),
           # points allowed per 100 possessions on defense by winning team.  
           Wdefense_efficiency = 100 * (LScore / Lpossessions),
           # difference in offense vs. defensive pts per 100 possessions
           Wnet_efficiency_ratio = round(((Woffense_efficiency / Wdefense_efficiency) -1), 3),
           Wscore_diff = WScore - LScore,
           # more offense related
           Wassist_to_fgm_ratio = WAst / WFGM,
           Weffective_fg_rate = (WFGM + (0.5 * WFGM3)) / WFGA,
           # Rebounding factors
           WORB_rate = WOR / (WOR + LDR),
           WDRB_rate = WDR / (LOR + WDR),
           # free throw factor
           WFT_rate = WFTA / WFGA,
           WFTM_pct = ifelse(is.nan(WFTM / WFTA), .689, WFTM / WFTA),
           # Turnover factor: turnovers per 100 possessions.
           Woffense_tov_per_100 = 100 *(WTO / (WFGA + (0.44 * WFTA) + WTO)),
           # Winner's defense stats. based on how the losing team performed in game. 
           Wdefense_effective_fg_rate = (LFGM + (0.5 * LFGM3)) / LFGA,
           Wdefense_FT_rate = LFTA / LFGA,
           Wdefense_tov_per_100 = 100 *(LTO / (LFGA + (0.44 * LFTA) + LTO)),
           Wopponent_ORB_rate = LOR / (LOR + WDR),
           Wopponent_DRB_rate = LDR / (WOR + LDR),
           # Foul Factors
           WPF_drawn_pg = LPF,
           WPF_committed_pg = WPF,  
           ########## LOSING TEAM's derived statistics ########## 
           Loffense_efficiency = 100 * (LScore / Lpossessions),
           Ldefense_efficiency = 100 * (WScore / Wpossessions),
           Lnet_efficiency_ratio = round(((Loffense_efficiency / Ldefense_efficiency) -1), 3),
           Lscore_diff = LScore - WScore,
           Lassist_to_fgm_ratio = LAst / LFGM,
           Leffective_fg_rate = (LFGM + (0.5 * LFGM3)) / LFGA,
           LORB_rate = LOR / (LOR + WDR),
           LDRB_rate = LDR / (WOR + LDR),
           LFT_rate = LFTA / LFGA,
           LFTM_pct = ifelse(is.nan(LFTM / LFTA), .689, LFTM / LFTA),
           Loffense_tov_per_100 = 100 *(LTO / (LFGA + (0.44 * LFTA) + LTO)),
           Ldefense_effective_fg_rate = (WFGM + (0.5 * WFGM3)) / WFGA,
           Ldefense_FT_rate = WFTA / WFGA,
           Ldefense_tov_per_100 = 100 *(WTO / (WFGA + (0.44 * WFTA) + WTO)),
           Lopponent_ORB_rate = WOR / (WOR + LDR), 
           Lopponent_DRB_rate = WDR / (LOR + WDR),
           LPF_drawn_pg = WPF,
           LPF_committed_pg = LPF)




# Normalize columns per team per season stats ###########################################

reg_season_details_winner <- reg_season_details %>%
    select(game_id, tournament_type, Season, DayNum, starts_with("W"), average_tempo) %>%
    rename_at(vars(starts_with("W")), # for columns starting with W
              function(x) stringr::str_sub(x, start = 2L, end = -1L)) %>% # strip W off column name.
    mutate(outcome = "W",
           outcome_1_0 = 1)

reg_season_details_loser <- reg_season_details %>%
    # need to create variable that codes the game location of the losing team, since the variable "Wloc" only codes winning team
    mutate(LLoc = ifelse(WLoc == "A", "H",
                         ifelse(WLoc == "H", "A", "N"))) %>%
    select(game_id, tournament_type, Season, DayNum, starts_with("L"), average_tempo) %>%
    rename_at(vars(starts_with("L")), # for columns starting with L
              function(x) stringr::str_sub(x, start = 2L, end = -1L)) %>% # strip off L. 
    mutate(outcome = "L",
           outcome_1_0 = 0)

# stack dataframes on top each other, sort by 
reg_season_details_long <- bind_rows(reg_season_details_winner,
                                     reg_season_details_loser) %>%
    arrange(TeamID, Season, DayNum)





# Convert per game stats to lagging team performance features ############################

reg_season_details_cumulative_stats <- reg_season_details_long %>%
    group_by(Team_name, Season) %>%
    # sort each team's game (row) chronological order
    arrange(Team_name, Season, DayNum) %>% 
    # these are 1 game lag cumulative averages, per team per season. I average each team’s stats by game (equal weighting by game). 
    mutate(season_wins = lag(cumsum(outcome_1_0)), # default lag of n = 1 prior period.
           season_win_rate = season_wins / lag(row_number()),
           possessions = lag(cummean(possessions)), 
           average_tempo = lag(cummean(average_tempo)),
           offense_efficiency = lag(cummean(offense_efficiency)),
           defense_efficiency = lag(cummean(defense_efficiency)),
           net_efficiency_ratio = lag(cummean(net_efficiency_ratio)),
           score_diff = lag(cummean(score_diff)),
           assist_to_fgm_ratio = lag(cummean(assist_to_fgm_ratio)),
           effective_fg_rate = lag(cummean(effective_fg_rate)),
           ORB_rate = lag(cummean(ORB_rate)),
           DRB_rate = lag(cummean(DRB_rate)),
           FT_rate = lag(cummean(FT_rate)),
           FTM_pct = lag(cummean(FTM_pct)),
           offense_tov_per_100 = lag(cummean(offense_tov_per_100)),
           defense_effective_fg_rate = lag(cummean(defense_effective_fg_rate)),
           defense_FT_rate = lag(cummean(defense_FT_rate)),
           defense_tov_per_100 = lag(cummean(defense_tov_per_100)),
           opponent_ORB_rate = lag(cummean(opponent_ORB_rate)),
           opponent_DRB_rate = lag(cummean(opponent_DRB_rate)),
           PF_drawn_pg = lag(cummean(PF_drawn_pg)), 
           PF_committed_pg = lag(cummean(PF_committed_pg))) %>%
    select(1:7, outcome, outcome_1_0, everything()) %>%
    ungroup()


# Identify 2014-2017 NCAA Tourney Participants ###########################################
# all possible game combinations of the 2018 tournament. per Kaggle rules. 
ncaa_participants_2018 <- s3read_using(FUN = read_csv, 
                                            object = "SampleSubmissionStage2.csv",
                                            bucket = "ncaabasketball") %>%
    select(ID) %>%
    tidyr::separate(ID, 
                    into = c("Season", "TeamID_1", "TeamID_2"), 
                    sep = "_",
                    remove = FALSE) %>%
    mutate(Season = as.integer(Season),
           TeamID_1 = as.integer(TeamID_1),
           TeamID_2 = as.integer(TeamID_2))


# Get 2014-2017 teams' last regular season game lagging team performance features ########

last_reg_season_game_2018 <- reg_season_details_cumulative_stats %>%
    filter(Season == 2018,
           tournament_type == "regular season") %>%
    group_by(Team_name, Season) %>%
    arrange(Team_name, Season, desc(DayNum)) %>%
    # select the row of the team's performance stats last game of season.
    slice(1) %>%
    ungroup() %>%
    select(-game_id, -tournament_type, -DayNum, -Team_name, -outcome,
           -outcome_1_0, -Score, -Loc, -FGM, -FGA, -FGM3, -FGA3, -FTM, -FTA, -OR,
           -DR, -Ast, -TO, -Stl, -Blk, -PF)


modeling_data_ncaa_2018 <- ncaa_participants_2018 %>%
    # left join the first team playing the game.
    left_join(last_reg_season_game_2018, by = c("Season" = "Season",
                                                "TeamID_1" = "TeamID")) %>%
    left_join(last_reg_season_game_2018, by = c("Season" = "Season",
                                                "TeamID_2" = "TeamID")) %>%
    rename_at(vars(ends_with(".x")), # for columns with .x (team outcome to predict)
              function(x) stringr::str_sub(x, start = 1L, end = -3L)) %>% # strip off the .x
    rename_at(vars(ends_with(".y")), # for columns with.y
              function(x) stringr::str_replace(x, "\\.y", "_OPP")) %>%
    mutate(conference_game_flag = Team_conf == Team_conf_OPP,
           possessions_DIFF = possessions - possessions_OPP, # basically the same as tempo_DIFF
           tempo_DIFF = average_tempo - average_tempo_OPP,
           offense_efficiency_DIFF = offense_efficiency - offense_efficiency_OPP,
           defense_efficiency_DIFF = defense_efficiency - defense_efficiency_OPP,
           net_efficiency_ratio_DIFF = net_efficiency_ratio - net_efficiency_ratio_OPP,
           score_margin_DIFF = score_diff - score_diff_OPP,
           assist_to_fgm_ratio_DIFF = assist_to_fgm_ratio - assist_to_fgm_ratio_OPP,
           effective_fg_rate_DIFF = effective_fg_rate - effective_fg_rate_OPP,
           ORB_rate_DIFF = ORB_rate - ORB_rate_OPP,
           DRB_rate_DIFF = DRB_rate - DRB_rate_OPP,
           FT_rate_DIFF = FT_rate - FT_rate_OPP,
           FTM_pct_DIFF = FTM_pct - FTM_pct_OPP,
           offense_tov_per_100_DIFF = offense_tov_per_100 - offense_tov_per_100_OPP,
           defense_effective_fg_rate_DIFF = defense_effective_fg_rate - defense_effective_fg_rate_OPP,
           defense_FT_rate_DIFF = defense_FT_rate - defense_FT_rate_OPP,
           defense_tov_per_100_DIFF = defense_tov_per_100 - defense_tov_per_100_OPP,
           # Difference in how each team allow's their opposition from getting ORB's.
           opponent_ORB_rate_DIFF = opponent_ORB_rate - opponent_ORB_rate_OPP,
           opponent_DRB_rate_DIFF = opponent_DRB_rate - opponent_DRB_rate_OPP,
           PF_drawn_pg_DIFF = PF_drawn_pg - PF_drawn_pg_OPP,
           PF_committed_pg_DIFF = PF_committed_pg - PF_committed_pg_OPP,
           season_wins_DIFF = season_wins - season_wins_OPP,
           season_win_rate_DIFF = season_win_rate - season_win_rate_OPP,
           POM_rank_DIFF = POM_rank_OPP - POM_rank, # how much better is the team ranked than opp. 
           RPI_rank_DIFF = RPI_rank_OPP - RPI_rank,
           SAG_rank_DIFF = SAG_rank_OPP - SAG_rank) %>%
    select(-starts_with("Team_conf"), -starts_with("TeamID"), -Season, 
           -possessions, # wasn't selected by random feature sampling in rf_fit_2003_2018
           -possessions_OPP) # wasn't selected by random feature sampling in rf_fit_2003_2018

# check no NAs or NANs in any data elements
sum(complete.cases(modeling_data_ncaa_2018)) == nrow(modeling_data_ncaa_2018)

# Run model through rf_fit_2003_2018. This is the model used for Kaggle Competition as of 3/13/18.
s3load(object = "rf_fit_2003_2018.Rdata", bucket = "ncaabasketball")

#check that names of features in the model agree with names in the modeling dataset.
names(modeling_data_ncaa_2018) %in% names(rf_fit_2003_2018$trainingData)

#########################################################################################
# predict the outcome of the 2018 game combinations using rf_fit_2003_2018
#########################################################################################
rf_preds_2018 <- predict(rf_fit_2003_2018, 
                             newdata = modeling_data_ncaa_2018[, -1],
                             type = "prob")

# combine Game ID with 2018 NCAA predictions. Prediction is for 1st team ID to beat 2nd team ID.
rf_preds_2018 <- modeling_data_ncaa_2018[1] %>%
    mutate(Pred = rf_preds_2018$W)

rf_preds_2018_submission1 <- rf_preds_2018 # submission on 3/13/2018 11pm.
# write submission file for Kaggle to AWS S3. 
s3write_using(x = rf_preds_2018_submission1, 
              FUN = write.csv,
              bucket = "ncaabasketball",
              object = "rf_preds_2018_submission1.csv")



#########################################################################################
# FOR FUN: get team details for predicted the outcome of the 2018 game combinations using rf_fit_2003_2018
#########################################################################################


rf_preds_2018_submission1_detailed <- ncaa_participants_2018 %>%
    inner_join(team_name, by = c("TeamID_1" = "TeamID")) %>%
    select(-ends_with("D1Season")) %>%
    inner_join(team_name, by = c("TeamID_2" = "TeamID")) %>%
    select(-ends_with("D1Season")) %>%
    inner_join(rf_preds_2018, by = c("ID" = "ID")) %>%
    rename(Team_1_Prediction = Pred,
           TeamName_1 = TeamName.x,
           TeamName_2 = TeamName.y) %>%
    select(ID, Season, Team_1_Prediction, TeamID_1, TeamName_1, TeamID_2, TeamName_2)

s3write_using(x = rf_preds_2018_submission1_detailed, 
              FUN = write.csv,
              bucket = "ncaabasketball",
              object = "rf_preds_2018_submission1_detailed.csv")





#########################################################################################
# predict the outcome of the 2018 game combinations using LASSO_fit_2003_2018
#########################################################################################
# Run model through rf_fit_2003_2018. This is the model used for Kaggle Competition as of 3/13/18.
s3load(object = "LASSO_fit_2003_2018.Rdata", bucket = "ncaabasketball")

#check that names of features in the model agree with names in the modeling dataset.
names(modeling_data_ncaa_2018) %in% LASSO_fit_2003_2018$xNames

LASSO_preds_2018 <- predict(LASSO_fit_2003_2018, 
                            s = LASSO_fit_2003_2018$lambdaOpt,
                            newx = as.matrix(modeling_data_ncaa_2018[, -1]),
                            type = "response")

# combine Game ID with 2018 NCAA predictions. Prediction from LASSO model is for 1st team ID to beat 2nd team ID.
LASSO_preds_2018 <- cbind(modeling_data_ncaa_2018[1], LASSO_preds_2018) %>%
    rename(Pred = "1")

LASSO_preds_2018_submission1 <- LASSO_preds_2018 # submission on 3/14/2018 10:40pm.
# write submission file for Kaggle to AWS S3. 
s3write_using(x = LASSO_preds_2018_submission1, 
              FUN = write.csv,
              bucket = "ncaabasketball",
              object = "LASSO_preds_2018_submission1.csv")



#########################################################################################
# Compare 2018 tourney predictions between RF_fit_2003_2018 and LASSO_fit_2003_2018
#########################################################################################

rf_preds_2018_submission1 <- s3read_using(FUN = read_csv, 
                                object = "rf_preds_2018_submission1.csv",
                                bucket = "ncaabasketball")

png(filename="2018 Preds - LASSO vs Random Forest Scatterplot.png",
    width = 600, height = 600)
plot(rf_preds_2018_submission1$Pred, LASSO_preds_2018_submission1$Pred)
dev.off()
