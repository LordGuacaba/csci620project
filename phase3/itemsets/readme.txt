to run this script you will need to run from root of project

python -m phase3.itemsets.apriori

this ensures that it is run as a module


min supports tuned by run PITCHES, POSITIONS, COMBOS
min = 2                     -> WAY too many, explosion
min = 100                   -> still way too many, explosion
min = 100000, 8000, 500     -> based on earlier runs for pitches and positions, still working on getting combos to actually run