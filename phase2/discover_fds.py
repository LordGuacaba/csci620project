import itertools
import pandas as pd
import psycopg2


def discover_all_fds(table_name, max_level=None):
    """
    Discover all functional dependencies that hold in a table using the lattice method.

    Args:
        table_name (str): name of the table in the DB
        max_level (int): optional limit for lattice depth (useful for large tables)

    Returns:
        list of tuples (X, Y) where X -> Y
    """
    params = dict(
        host="localhost",
        dbname="baseball_db",
        user="postgres",
        password="$nax459:)",
    )

    print(f"\n=== Discovering FDs for table: {table_name} ===")

    conn = psycopg2.connect(**params)
    # get all data from the table into a DataFrame
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()

    all_attrs = set(df.columns)
    all_fds = []
    level = 1

    # lattice traversal
    while level <= len(all_attrs):
        subsets = list(itertools.combinations(all_attrs, level))
        if max_level and level > max_level:
            break
        print(f"\nlevel {level} ({len(subsets)} subsets")

        for subset in subsets:
            X = set(subset)
            closure = compute_closure(df, X, all_attrs)
            new_deps = closure - X
            for Y in new_deps:
                all_fds.append((tuple(sorted(X)), Y))
                print(f"  {'{' + ', '.join(X) + '}'} → {Y}")

        level += 1

    print(f"\n Found {len(all_fds)} total FDs in {table_name}")
    return all_fds


def compute_closure(df, X, all_attrs):
    """
    Compute the closure of attribute set X in DataFrame df.
    Args:
        df (pd.DataFrame): data table
        X (set): set of attributes
        all_attrs (set): all attributes in the table
    """
    closure = set(X)
    changed = True
    while changed:
        changed = False
        for Y in all_attrs - closure:
            grouped = df.groupby(list(closure))[Y].nunique(dropna=False)
            if grouped.max() == 1:
                closure.add(Y)
                changed = True
    return closure


def main():
    tables_to_check = ["Teams", "Players", "Ballparks", "Games", "PlayerActivity"]

    fd_summary = {}

    for table in tables_to_check:
        fds = discover_all_fds(table)
        fd_summary[table] = fds

    # WRITE RESULTS
    output_rows = []
    for table, fds in fd_summary.items():
        for lhs, rhs in fds:
            output_rows.append(
                {"Table": table, "Determinant": ", ".join(lhs), "Dependent": rhs}
            )

    # save as csv
    df = pd.DataFrame(output_rows)
    output_path = "phase2_fd_results.csv"
    df.to_csv(output_path, index=False)
    print(f"\nsaved all fd results to {output_path}")

    # write as readable text
    with open("phase2_fd_results.txt", "w") as f:
        for table, fds in fd_summary.items():
            f.write(f"\nTable: {table}\n")
            if fds:
                for lhs, rhs in fds:
                    f.write(f"  {', '.join(lhs)} → {rhs}\n")
            else:
                f.write("  no fds\n")

    print("also wrote human readable results to phase2_fd_results.txt")

    # summary printout
    print("\nfunctional dependency summary")
    for table, fds in fd_summary.items():
        print(f"\ntable: {table} ({len(fds)} fds)")
        for lhs, rhs in fds:
            print(f"  {', '.join(lhs)} → {rhs}")


if __name__ == "__main__":
    main()
