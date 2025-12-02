to run this script you will need to run from root of project

python -m phase3.itemsets.apriori

this ensures that it is run as a module


############ min supports tuned by run PITCHES, POSITIONS, COMBOS ##############3
min = 2                     -> WAY too many, explosion
min = 100                   -> still way too many, explosion
min = 100000, 8000, 500     -> based on earlier runs for pitches and positions, still working on getting combos to actually 
                               500 for combos is still way too small
min = 100000, 8000, 1000.   -> pitches and positions are good. finding combos threshold is proving irritating. Hard to separate 
                               number of games played individually from number of games played together. 

min = 2000 combos           -> upping to 2000 for combos because its still way too many combos. 2000 was good

min = 3800 positions        -> removed batting positions because theyre trivial and reducing min support to include more fielding combos




########## intuition for future scott: ###############
support - how common an itemset is in the database
confidence - how often does X appear when Y appears (not as meaningful as lift becauase Y might be extremely common)
lift - how much more likely Y is when X happens compared to random chance (determins how meaningful it is)


########## issues with combos: ##################
- number of unique items is so large, causes an explosion combinatorically
- going to need to reduce max k, increase min support, and/or limit to certain teams


########## issues with positions ##################
- was including batting positions which was causing my min support to go high in order to compensate for bloat
- removed batting position and lowered min to 3800 in order to get fielding positions
