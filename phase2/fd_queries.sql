-- file to find erroneous fds from tables


--- check for distinct values in each fd column
--hometeam_vals  visteam_vals  date_vals  location_vals  usedh_vals  htbf_vals  attendance_vals  win_vals  lose_vals  sv_vals
--44                44          19448             99           2          1            47929      6598       7178     4145
SELECT 
    COUNT(DISTINCT hometeam) AS hometeam_vals,
    COUNT(DISTINCT visteam) AS visteam_vals,
    COUNT(DISTINCT date) AS date_vals,
    COUNT(DISTINCT location) AS location_vals,
    COUNT(DISTINCT usedh) AS usedh_vals,
    COUNT(DISTINCT htbf) AS htbf_vals,
    COUNT(DISTINCT attendance) AS attendance_vals,
    COUNT(DISTINCT winningpitcher) AS win_vals,
    COUNT(DISTINCT losingpitcher) AS lose_vals,
    COUNT(DISTINCT sv) AS sv_vals
FROM Games;

-- check for number of occurrences of each value in htbf column since its causing issues
--    htbf   count
-- 0  True      47
-- 1  None  193649
SELECT htbf, COUNT(*) 
FROM Games 
GROUP BY htbf;


-- check usedh column for number of occurrences of each value
--    usedh   count
-- 0  False  131965
-- 1   True   61731
SELECT usedh, COUNT(*) 
FROM Games 
GROUP BY usedh;


-- check fieldpos -> pinchHit
-- check fieldpos -> pinchRun
--     fieldingpos  any_pinchhit  any_pinchrun    count
-- 0             1         False         False  1126367
-- 1             2         False         False   446479
-- 2             3         False         False   436097
-- 3             4         False         False   435448
-- 4             5         False         False   436325
-- 5             6         False         False   431395
-- 6             7         False         False   465670
-- 7             8         False         False   435278
-- 8             9         False         False   449372
-- 9            10         False         False   123437
-- 10           11          True         False   442027
-- 11           12         False          True    80937
SELECT
    fieldingPos,
    BOOL_OR(pinchHit) AS any_pinchHit,
    BOOL_OR(pinchRun) AS any_pinchRun,
    COUNT(*) AS count
FROM PlayerActivity
GROUP BY fieldingPos
ORDER BY fieldingPos;


