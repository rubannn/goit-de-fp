CREATE DATABASE IF NOT EXISTS olympic_dataset
    CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

CREATE TABLE olympic_dataset.athlete_event_results (
    edition VARCHAR(64),
    edition_id INT,
    country_noc VARCHAR(3),
    sport VARCHAR(64),
    event VARCHAR(255),
    result_id BIGINT,
    athlete VARCHAR(255),
    athlete_id BIGINT,
    pos VARCHAR(32),
    medal VARCHAR(16),
    isTeamSport VARCHAR(5),
    INDEX idx_medal (medal)
);

LOAD DATA INFILE '/var/lib/mysql-files/athlete_event_results.csv'
INTO TABLE olympic_dataset.athlete_event_results
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(edition, edition_id, country_noc, sport, event, result_id,
 athlete, athlete_id, pos, @medal, @is_team_sport)
SET medal = NULLIF(@medal, ''),
    isTeamSport = TRIM(TRAILING '\r' FROM @is_team_sport);
