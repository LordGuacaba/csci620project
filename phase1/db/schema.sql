DROP TABLE IF EXISTS AtBats;
DROP TABLE IF EXISTS PlayerActivity;
DROP TABLE IF EXISTS Games;
DROP TABLE IF EXISTS Players;
DROP TABLE IF EXISTS Ballparks;
DROP TABLE IF EXISTS Teams;

CREATE TABLE Teams (
    id CHAR(3) PRIMARY KEY NOT NULL,
    city VARCHAR(50) NOT NULL,
    name VARCHAR(50) NOT NULL,
    first INT NOT NULL,
    last INT
);

CREATE TABLE Ballparks (
    id CHAR(5) PRIMARY KEY NOT NULL,
    name VARCHAR(50),
    city VARCHAR(50),
    state CHAR(2)
);

CREATE TABLE Players (
    id CHAR(8) PRIMARY KEY NOT NULL,
    firstName VARCHAR(50),
    lastName VARCHAR(50),
    DOB DATE,
    bats CHAR(1),
    throws CHAR(1)
);

CREATE TABLE Games (
    id VARCHAR(50) PRIMARY KEY NOT NULL,
    homeTeam CHAR(3),
    visTeam CHAR(3),
    date DATE,
    location CHAR(5),
    useDH BOOLEAN,
    htbf BOOLEAN,
    attendance INT,
    winningPitcher CHAR(8),
    losingPitcher CHAR(8),
    sv CHAR(8)
    FOREIGN KEY (homeTeam) REFERENCES Teams(id),
    FOREIGN KEY (visTeam) REFERENCES Teams(id),
    FOREIGN KEY (location) REFERENCES Ballparks(id),
    FOREIGN KEY (winningPitcher) REFERENCES Players(id),
    FOREIGN KEY (losingPitcher) REFERENCES Players(id),
    FOREIGN KEY (sv) REFERENCES Players(id)
);

CREATE TABLE PlayerActivity (
    id BIGINT PRIMARY KEY NOT NULL,
    gameId VARCHAR(50) NOT NULL,
    playerId CHAR(8) NOT NULL,
    team CHAR(3) NOT NULL,
    battingPos INT,
    fieldingPos INT,
    inning INT,
    pinchHit BOOLEAN,
    pinchRun BOOLEAN
    FOREIGN KEY (gameId) REFERENCES Games(id),
    FOREIGN KEY (playerId) REFERENCES Players(id),
    FOREIGN KEY (team) REFERENCES Teams(id)
);

CREATE TABLE AtBats (
    num INT NOT NULL,
    game VARCHAR(15) NOT NULL,
    batter CHAR(8) NOT NULL,
    inning INT,
    top_bottom CHAR(1),
    pitches VARCHAR(50),
    play VARCHAR(50),
    playDetails VARCHAR(50),
    baseRunnerDetails VARCHAR(50),
    PRIMARY KEY (num, game)
    FOREIGN KEY (game) REFERENCES Games(id),
    FOREIGN KEY (batter) REFERENCES Players(id)
);