-- 1. All 58 of Ryan Howard’s home runs from his 2006 MVP season:
select * from atbats where batter = 'howar001' and game like '%2006%' and play = 'HR';

-- 2. Games during which the home team’s batters combined to strike out at least 20 times:
select game, count(*) from atbats where play like 'K%' and top_bottom = 'B'
group by game having count(*) >= 20 order by count(*);

-- 3. Games during which the visiting team used at least 10 different pitchers:
select distinct gameid, count(*) from playeractivity pa
join games g on g.id = pa.gameid
where pa.team = g.visteam and pa.fieldingpos = 1
group by gameid, team, fieldingpos
having count(*) >= 10;

-- 4. 4 Home Run Games
select g.date, game, batter, p.firstname, p.lastname, count(*) as hr from atbats a
join games g on g.id = a.game
join players p on p.id = a.batter
where play like 'HR%'
group by g.date, game, batter, p.firstname, p.lastname
having count(*) >= 4
order by g.date;

-- 5. Times a catcher has hit for the cycle (single, double, triple and home run in the same game)
select g.date, game, batter, p.firstname, p.lastname,
count(case when play like 'S%' and play not like 'SB%' then 1 end) as singles,
count(case when play like 'D%' then 1 end) as doubles,
count(case when play like 'T%' then 1 end) as triples,
count(case when play like 'HR%' then 1 end) as hrs from atbats a
join games g on a.game = g.id
join playeractivity pa on pa.gameid = g.id and pa.playerid = batter
join players p on a.batter = p.id
where pa.fieldingpos = 2
group by g.date, game, batter, p.firstname, p.lastname
having count(case when play like 'S%' and play not like 'SB%' then 1 end) >= 1 
and count(case when play like 'D%' then 1 end) >= 1
and count(case when play like 'T%' then 1 end) >= 1
and count(case when play like 'HR%' then 1 end) >= 1
order by g.date;

-- indexes used:
CREATE INDEX idx_atbats_game ON atbats(game);
CREATE INDEX idx_atbats_batter ON atbats(batter);
CREATE INDEX idx_atbats_play ON atbats(play);
CREATE INDEX idx_atbats_game_trgm ON atbats USING gin (game gin_trgm_ops);
CREATE INDEX idx_playeractivity_gameid ON playeractivity(gameid);
CREATE INDEX idx_playeractivity_playerid ON playeractivity(playerid);
CREATE INDEX idx_playeractivity_fieldingpos ON playeractivity(fieldingpos);