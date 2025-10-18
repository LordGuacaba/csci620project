def recursiverly_read_directory(path, file_list=None):
    """
    Recursively reads a directory and returns a list of all file paths within it
    """
    import os

    DIR = "../data/eventFiles"

    if file_list is None:
        file_list = []

    for entry in os.scandir(path):
        if entry.is_file():
            if entry.path.endswith(".EVA") or entry.path.endswith(".EVN"):
                file_list.append(entry.path)
        elif entry.is_dir():
            recursiverly_read_directory(entry.path, file_list)

    return file_list


def main():
    import os

    DIR = "../data/eventFiles"
    all_files = recursiverly_read_directory(DIR)
    for file_path in all_files:
        print(file_path)


if __name__ == "__main__":
    main()
