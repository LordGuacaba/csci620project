import time
from itertools import combinations
import pandas as pd
import psycopg2


DB_PARAMS = dict(
    host="localhost",
    dbname="baseball_db",
    user="postgres",
    password="$nax459:)",
)


def build_single_column_partitions(df):
    partitions = {}
    for col in df.columns:
        groups = df.groupby(col, dropna=False, sort=False).groups
        partitions[col] = [set(idx_list.tolist()) for idx_list in groups.values()]
    return partitions


def compute_partition_for_alpha(df, alpha):
    groups = df.groupby(list(alpha), dropna=False, sort=False).groups
    return [set(idx_list.tolist()) for idx_list in groups.values()]


def partition_refines(partition_alpha, partition_b):
    rhs_id = {}
    for idx, block in enumerate(partition_b):
        for r in block:
            rhs_id[r] = idx

    for block_a in partition_alpha:
        it = iter(block_a)
        first = next(it, None)
        if first is None:
            continue
        bid = rhs_id[first]
        for r in it:
            if rhs_id[r] != bid:
                return False
    return True


def generate_next_level(prev_level):
    next_level = []
    prev_sorted = sorted([tuple(sorted(alpha)) for alpha in prev_level])

    for i in range(len(prev_sorted)):
        for j in range(i + 1, len(prev_sorted)):
            L1 = prev_sorted[i]
            L2 = prev_sorted[j]

            if L1[:-1] == L2[:-1]:
                cand = tuple(sorted(set(L1) | set(L2)))
                next_level.append(cand)

    return list(dict.fromkeys(next_level))


def discover_functional_dependencies_lattice_pruning(
    table_name, max_level=None, db_params=None
):
    params = db_params or DB_PARAMS

    print(f"\ndiscovering fds for table: {table_name}")

    conn = psycopg2.connect(**params)
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()

    if df.empty:
        return []

    df = df.fillna("<NULL>")
    attrs = df.columns.tolist()

    single_partitions = build_single_column_partitions(df)

    fds = []
    fds_set = set()

    current_level = [(a,) for a in attrs]
    level_num = 1

    while current_level:
        print(
            f"\n-- level {level_num} ({len(current_level)} attribute sets) ------------------"
        )

        next_level = []

        for alpha in current_level:
            alpha_set = set(alpha)
            partition_alpha = compute_partition_for_alpha(df, alpha)

            for b in attrs:
                if b in alpha_set:
                    continue

                skip = False
                for r in range(1, len(alpha)):
                    for beta in combinations(alpha, r):
                        if (tuple(sorted(beta)), b) in fds_set:
                            skip = True
                            break
                    if skip:
                        break
                if skip:
                    continue

                partition_b = single_partitions[b]

                if partition_refines(partition_alpha, partition_b):
                    fds.append((alpha, b))
                    fds_set.add((tuple(sorted(alpha)), b))
                    print(f"FD FOUND: {alpha} -> {b}")

            next_level.append(alpha)

        if max_level and level_num >= max_level:
            break

        current_level = generate_next_level(current_level)
        level_num += 1

    return fds


def write_fd_output(table_name, fds):
    filename = f"fds_{table_name}.txt"
    with open(filename, "w") as f:
        f.write(f"fds for table: {table_name}\n")
        f.write("=" * 70 + "\n\n")

        if not fds:
            f.write("(No functional dependencies found)\n")
            return

        for alpha, b in fds:
            lhs_str = ", ".join(alpha)
            f.write(f"{lhs_str} -> {b}\n")

    print(f"saved {filename}")


def main():
    tables = ["AtBats"]

    for table in tables:
        fds = discover_functional_dependencies_lattice_pruning(table, max_level=3)
        write_fd_output(table, fds)


if __name__ == "__main__":
    main()
