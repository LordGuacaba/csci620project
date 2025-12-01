import sys
import pandas as pd
from phase3.db.config import connect
from collections import defaultdict
import time
from itertools import combinations


def generate_rules(all_itemsets, num_transactions, min_confidence=0.1):
    """
    Generate association rules from the frequent itemsets produced by Apriori.

    Parameters:
    - all_itemsets: dict mapping level k → { itemset: support }
    - min_confidence: minimum confidence threshold (float)

    Returns:
    - rules: list of dicts {X, Y, support, confidence, lift}
    """

    # Flatten all itemsets into a single support lookup
    support_lookup = {}
    for level in all_itemsets.values():
        for itemset, sup in level.items():
            support_lookup[itemset] = sup

    rules = []

    for itemset, itemset_support in support_lookup.items():

        # itemsets of size >= 2
        if len(itemset) < 2:
            continue

        items = list(itemset)

        # X is any nonempty proper subset
        # Y = itemset - X
        # for all possible sizes of X
        for r in range(1, len(items)):
            # all combinations of size r
            for X_tuple in combinations(items, r):
                X = frozenset(X_tuple)
                Y = itemset - X

                support_X = support_lookup.get(X, None)
                support_Y = support_lookup.get(Y, None)

                # If the subset was not in the frequent itemsets, skip
                if support_X is None or support_Y is None:
                    continue

                confidence = itemset_support / support_X
                lift = confidence / (support_Y / num_transactions)

                if confidence >= min_confidence:
                    rules.append(
                        {
                            "X": X,
                            "Y": Y,
                            "support": itemset_support,
                            "confidence": confidence,
                            "lift": lift,
                        }
                    )

    # sort by lift desc, then confidence desc
    rules.sort(key=lambda r: (-r["lift"], -r["confidence"]))

    return rules


def write_rules_to_file(rules, filename_prefix="association_rules"):
    """
    Write association rules to an output text file.
    """

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    filename = f"{filename_prefix}_{timestamp}.txt"

    with open(filename, "w") as f:
        for rule in rules:
            X = ", ".join(sorted(rule["X"]))
            Y = ", ".join(sorted(rule["Y"]))
            f.write(
                f"{X}  -->  {Y}  "
                f"(support={rule['support']}, "
                f"confidence={rule['confidence']:.3f}, "
                f"lift={rule['lift']:.3f})\n"
            )

    print(f"Saved association rules to: {filename}")
    return filename


def progress_bar(current, total, prefix="", length=40):
    """
    Display a progress bar in the console.
    Parameters:
    - current: Current progress (int)
    - total: Total value for completion (int)
    - prefix: Prefix string for the progress bar (str)
    - length: Length of the progress bar (int)
    """
    # this works beautifully, i should have made this way earlier in my life
    percent = current / total
    filled = int(length * percent)
    bar = "#" * filled + "-" * (length - filled)
    # fancy bar
    sys.stdout.write(f"\r{prefix} |{bar}| {percent*100:5.1f}%")
    # flush to ensure it prints immediately
    sys.stdout.flush()


def count_support(transactions, candidates):
    """
    Count the support for each candidate itemset in the transactions.
    Parameters:
    - transactions: List of transactions (list of sets)
    - candidates: Candidate itemsets to count support for (set of frozensets)
    Returns:
    - support: Dictionary mapping itemsets to their support count
    """
    support = defaultdict(int)
    total = len(candidates)
    for i, candidate in enumerate(candidates, start=1):
        progress_bar(i, total, prefix="Counting support")
        for tran in transactions:
            if candidate.issubset(tran):
                support[candidate] += 1
    print()
    return support


def generate_candidates(Lk):
    """
    Generate candidate itemsets for the next level (C(k+1)) from the current frequent itemsets (Lk).
    """
    Lk_list = list(Lk.keys())
    candidates = set()

    for i in range(len(Lk_list)):
        for j in range(i + 1, len(Lk_list)):
            a = list(Lk_list[i])
            b = list(Lk_list[j])

            a.sort()
            b.sort()

            # join if k-1 items are the same
            if a[:-1] == b[:-1]:
                candidate = frozenset(set(a) | set(b))
                candidates.add(candidate)

    return candidates


def full_apriori(transactions, min_support, max_k=None):
    """
    Full Apriori algorithm to find all frequent itemsets in the given transactions.
    Parameters:
    - transactions: List of transactions (list of lists)
    - min_support: Minimum support threshold (int)
    Returns:
    - all_levels: Dictionary mapping level k to frequent itemsets at that level
    """
    # convert for speed
    transactions = [set(tran) for tran in transactions]

    # L1
    item_counts = defaultdict(int)
    for tran in transactions:
        for item in tran:
            item_counts[frozenset([item])] += 1

    L1 = {
        itemset: count for itemset, count in item_counts.items() if count >= min_support
    }

    all_levels = {1: L1}
    print(f"L1 frequent itemsets: {len(L1)}")

    k = 1

    # Lk
    while True:
        k += 1

        # stop if max_k reached
        if max_k is not None and k > max_k:
            break

        # Ck candidate generation
        ck = generate_candidates(all_levels[k - 1])

        if not ck:
            # if no candidates, stop
            break

        # support count
        print(f"Generating C{k} with {len(ck)} candidates...")
        support_counts = count_support(transactions, ck)

        # frequent itemsets
        lk = {
            itemset: count
            for itemset, count in support_counts.items()
            if count >= min_support
        }

        if not lk:
            # if no frequent, stop
            break

        all_levels[k] = lk

    return all_levels


def write_itemsets_to_file(all_itemsets, filename_prefix="apriori_output"):
    """
    Write the frequent itemsets to a timestamped text file.
    Parameters:
    - all_itemsets: Dictionary mapping level k to frequent itemsets at that level
    - filename_prefix: Prefix for the output filename (str)
    Returns:
    - filename: The name of the file where itemsets were saved (str)
    """
    # PSUEDO-UNIQUE FILENAME
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    filename = f"{filename_prefix}_{timestamp}.txt"

    with open(filename, "w") as f:
        for k, level in sorted(all_itemsets.items()):
            for itemset, count in sorted(level.items(), key=lambda x: (-x[1], x[0])):
                f.write(f"{set(itemset)} : {count}\n")

    print(f"\nsaved results to: {filename}")
    return filename


def frequent_pitch_transactions(table_df):
    """
    Build transactions based on pitch types from playeractivity table dataframe.
    Each transaction is a list of pitch types for a single player activity.
    """
    transactions = []
    # im not sure if i want to be more granular here and parse the fields more.
    # i probably will not unless absolutely necessary
    for _, row in table_df.iterrows():
        pitches = row["pitches"]

        if pitches is None or pd.isna(pitches) or pitches == "":
            continue

        tran = [f"pitch_{p}" for p in pitches.strip()]
        transactions.append(tran)

    return transactions


def common_position_groups(table_df):
    """
    Build transactions based on position groups from playeractivity table dataframe.
    Each transaction is a list of positions played by a player in a single game.
    """
    transactions = []
    grouped = table_df.groupby("playerid")

    for pid, group in grouped:
        tran = [f"player_{pid}"]

        # Defensive positions
        for pos in group["fieldingpos"].dropna().unique():
            tran.append(f"fieldpos_{int(pos)}")

        # Batting positions
        for pos in group["battingpos"].dropna().unique():
            tran.append(f"batpos_{int(pos)}")

        transactions.append(tran)

    return transactions


def long_lived_player_combos(table_df):
    """
    Build transactions based on player combinations from playeractivity table dataframe.
    Each transaction is a list of player IDs that played together in the same game.
    """

    transactions = []
    grouped = table_df.groupby(["gameid", "team"])

    for (gameid, team), group in grouped:
        tran = [f"team_{team}"]
        tran += [f"player_{pid}" for pid in group["playerid"].unique()]
        transactions.append(tran)

    return transactions


def main():
    # get min_support from command line args or default to 2
    # i was originally going to make this configurable but since im running it as a module im not sure
    min_support = int(sys.argv[1]) if len(sys.argv) > 1 else 2

    # connect to database
    conn = connect()

    # get pandas dataframe from sql queries of tables
    # teams, ballparks, players, games, playerActivities, atBats
    player_activities = pd.read_sql("SELECT * FROM playeractivity;", conn)
    at_bats = pd.read_sql("SELECT * FROM atbats;", conn)
    conn.close()

    # transactions
    frequent_pitches = frequent_pitch_transactions(at_bats)
    common_positions = common_position_groups(player_activities)
    player_combos = long_lived_player_combos(player_activities)

    # apriori algorithm implementation
    pitch_itemsets = full_apriori(frequent_pitches, min_support, max_k=4)
    write_itemsets_to_file(pitch_itemsets, filename_prefix="pitch_itemsets")
    pitch_rules = generate_rules(
        pitch_itemsets, num_transactions=len(frequent_pitches), min_confidence=0.2
    )
    write_rules_to_file(pitch_rules, "pitch_rules")

    position_itemsets = full_apriori(common_positions, min_support, max_k=4)
    write_itemsets_to_file(position_itemsets, filename_prefix="position_itemsets")
    position_rules = generate_rules(
        position_itemsets, num_transactions=len(common_positions), min_confidence=0.2
    )
    write_rules_to_file(position_rules, "position_rules")

    combo_itemsets = full_apriori(player_combos, min_support, max_k=4)
    write_itemsets_to_file(combo_itemsets, filename_prefix="combo_itemsets")
    combo_rules = generate_rules(
        combo_itemsets, num_transactions=len(player_combos), min_confidence=0.2
    )
    write_rules_to_file(combo_rules, "combo_rules")


if __name__ == "__main__":
    main()
