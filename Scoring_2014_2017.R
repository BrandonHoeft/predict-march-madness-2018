#########################################################################################
# Script for scoring the 2014 - 2017 NCAA Tourney participant games
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
ncaa_participants_2014_2017 <- s3read_using(FUN = read_csv, 
                          object = "SampleSubmissionStage1.csv",
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

last_reg_season_game_2014_2017 <- reg_season_details_cumulative_stats %>%
    filter(Season %in% 2014:2017,
           tournament_type == "regular season") %>%
    group_by(Team_name, Season) %>%
    arrange(Team_name, Season, desc(DayNum)) %>%
    # select the row of the team's performance stats last game of season.
    slice(1) %>%
    ungroup() %>%
    select(-game_id, -tournament_type, -DayNum, -Team_name, -outcome,
           -outcome_1_0, -Score, -Loc, -FGM, -FGA, -FGM3, -FGA3, -FTM, -FTA, -OR,
           -DR, -Ast, -TO, -Stl, -Blk, -PF)

    
modeling_data_ncaa_2014_2017 <- ncaa_participants_2014_2017 %>%
    # left join the first team playing the game.
    left_join(last_reg_season_game_2014_2017, by = c("Season" = "Season",
                                                     "TeamID_1" = "TeamID")) %>%
    left_join(last_reg_season_game_2014_2017, by = c("Season" = "Season",
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
    select(-starts_with("Team_conf"), -starts_with("TeamID"), -Season, -possessions,
           -possessions_OPP)

# check no NAs or NANs
sum(complete.cases(modeling_data_ncaa_2014_2017)) == nrow(modeling_data_ncaa_2014_2017)

# Run model through rf_fit_2003_2013
s3load(object = "rf_fit_2003_2013.Rdata", bucket = "ncaabasketball")



#########################################################################################
# predict the outcome of the 2014-2017 game combinations using rf_fit_2003_2013
#########################################################################################
rf_pred_2014_2017 <- predict(rf_fit_2003_2013, 
                             newdata = modeling_data_ncaa_2014_2017[, -1],
                             type = "prob")

# combine Game ID with predictions
rf_pred_2014_2017 <- modeling_data_ncaa_2014_2017[1] %>%
    mutate(Pred = rf_pred_2014_2017$W)


actual_wins <- NCAATourneyDetailedResults_Pre2018 %>%
    inner_join(team_name, by = c("WTeamID" = "TeamID")) %>%
    select(-ends_with("D1Season")) %>%
    inner_join(team_name, by = c("LTeamID" = "TeamID")) %>%
    select(-ends_with("D1Season")) %>%
    filter(Season %in% 2014:2017) %>%
    mutate(game_id = paste(Season, WTeamID, LTeamID, sep = "_"),
           outcome = "W") %>%
    select(game_id, outcome, TeamName.x, WScore, TeamName.y, LScore) %>%
    rename(TeamName1 = TeamName.x,
           Score1 = WScore,
           TeamName2 = TeamName.y,
           Score2 = LScore) %>%
    inner_join(rf_pred_2014_2017, by = c("game_id" = "ID")) %>% 
    arrange(Pred)
    
actual_losses <- NCAATourneyDetailedResults_Pre2018 %>%
    inner_join(team_name, by = c("WTeamID" = "TeamID")) %>%
    select(-ends_with("D1Season")) %>%
    inner_join(team_name, by = c("LTeamID" = "TeamID")) %>%
    select(-ends_with("D1Season")) %>%
    filter(Season %in% 2014:2017) %>%
    mutate(game_id = paste(Season, LTeamID, WTeamID, sep = "_"),
           outcome = "L") %>%
    select(game_id, outcome, TeamName.x, WScore, TeamName.y, LScore) %>%
    rename(TeamName1 = TeamName.x,
           Score1 = WScore,
           TeamName2 = TeamName.y,
           Score2 = LScore) %>%
    inner_join(rf_pred_2014_2017, by = c("game_id" = "ID")) %>% 
    arrange(Pred)

rf_pred_2014_2017_performance <- bind_rows(actual_wins, actual_losses) %>%
    arrange(outcome, Pred)

library(ggplot2)
ggplot(rf_pred_2014_2017_performance, aes(x = Pred, color = outcome)) + geom_density(alpha = 0.5) +
    scale_x_continuous(breaks = seq(0, 1, .1)) +
    labs(title = "Stage 1 Predictions by 2014-2017 NCAA Tourney Game Outcomes")

library(pROC)
auc(response = rf_pred_2014_2017_performance$outcome, 
    predictor = rf_pred_2014_2017_performance$Pred)
# AUC = 0.7726

library(MLmetrics)
LogLoss(y_pred = rf_pred_2014_2017_performance$Pred, 
        y_true = ifelse(rf_pred_2014_2017_performance$outcome == "W", 1, 0))
# Log loss = 0.5774036

s3write_using(x = rf_pred_2014_2017_performance, 
              FUN = write.csv,
              bucket = "ncaabasketball",
              object = "rf_pred_2014_2017_performance.csv")


#########################################################################################
# predict the outcome of the 2014-2017 game combinations using LASSO_pred_2014_2017
#########################################################################################
s3load(object = "LASSO_fit_2003_2013.Rdata", bucket = "ncaabasketball")

LASSO_pred_2014_2017 <- predict(LASSO_fit_2003_2013, 
                                s = LASSO_fit_2003_2013$lambdaOpt,
                                newx = as.matrix(modeling_data_ncaa_2014_2017[, -1]),
                                type = "response")
LASSO_pred_2014_2017 <- cbind(modeling_data_ncaa_2014_2017[1], LASSO_pred_2014_2017) %>%
    rename(Pred = "1")

actual_wins <- NCAATourneyDetailedResults_Pre2018 %>%
    inner_join(team_name, by = c("WTeamID" = "TeamID")) %>%
    select(-ends_with("D1Season")) %>%
    inner_join(team_name, by = c("LTeamID" = "TeamID")) %>%
    select(-ends_with("D1Season")) %>%
    filter(Season %in% 2014:2017) %>%
    mutate(game_id = paste(Season, WTeamID, LTeamID, sep = "_"),
           outcome = "W") %>%
    select(game_id, outcome, TeamName.x, WScore, TeamName.y, LScore) %>%
    rename(TeamName1 = TeamName.x,
           Score1 = WScore,
           TeamName2 = TeamName.y,
           Score2 = LScore) %>%
    inner_join(LASSO_pred_2014_2017, by = c("game_id" = "ID")) %>% 
    arrange(Pred)

actual_losses <- NCAATourneyDetailedResults_Pre2018 %>%
    inner_join(team_name, by = c("WTeamID" = "TeamID")) %>%
    select(-ends_with("D1Season")) %>%
    inner_join(team_name, by = c("LTeamID" = "TeamID")) %>%
    select(-ends_with("D1Season")) %>%
    filter(Season %in% 2014:2017) %>%
    mutate(game_id = paste(Season, LTeamID, WTeamID, sep = "_"),
           outcome = "L") %>%
    select(game_id, outcome, TeamName.x, WScore, TeamName.y, LScore) %>%
    rename(TeamName1 = TeamName.x,
           Score1 = WScore,
           TeamName2 = TeamName.y,
           Score2 = LScore) %>%
    inner_join(LASSO_pred_2014_2017, by = c("game_id" = "ID")) %>% 
    arrange(Pred)

LASSO_pred_2014_2017_performance <- bind_rows(actual_wins, actual_losses) %>%
    arrange(outcome, Pred)

library(ggplot2)
ggplot(LASSO_pred_2014_2017_performance, aes(x = Pred, color = outcome)) + geom_density(alpha = 0.5) +
    scale_x_continuous(breaks = seq(0, 1, .1)) +
    labs(title = "Stage 1 Predictions by 2014-2017 NCAA Tourney Game Outcomes")

library(pROC)
auc(response = LASSO_pred_2014_2017_performance$outcome, 
    predictor = LASSO_pred_2014_2017_performance$Pred)
# AUC = 0.7927

library(MLmetrics)
LogLoss(y_pred = LASSO_pred_2014_2017_performance$Pred, 
        y_true = ifelse(LASSO_pred_2014_2017_performance$outcome == "W", 1, 0))
# Log loss = 0.558

# how correlated are the Random Forest and LASSO regression? MODEL PREDICTIONS NEAR PERFECTLY CORRELATED.
plot(LASSO_pred_2014_2017_performance$Pred, rf_pred_2014_2017_performance$Pred)
cor(LASSO_pred_2014_2017_performance$Pred, rf_pred_2014_2017_performance$Pred)

bind_rows(LASSO_pred_2014_2017_performance,
          rf_pred_2014_2017_performance) %>%
    mutate(model = c(rep("LASSO", nrow(LASSO_pred_2014_2017_performance)),
                     rep("Random_Forest", nrow(rf_pred_2014_2017_performance)))) %>%
    tidyr::spread(model, Pred) %>%
ggplot(aes(x = LASSO,
           y = Random_Forest)) + geom_point() + 
    labs(title = "Correlation between Random Forest and LASSO",
         subtitle = "2014-2017 NCAA Tournament games (out of sample predictions)")
ggsave("LASSO vs RF correlation 2014-17 test data.png")

