# csci620project

CSCI 620 Project

# notes for data cleaning

- Players can have null/ NaN fields
  - handled in parsing in parsin/parse_csv_file_to_pandas_df function via pandas
- Players DoB can be in different formats. abree101 is '1901' but others are in date format
  - handled in parser for Players via
    - player.setValue("DOB", pd.to_datetime(row["BIRTHDATE"], errors="coerce"))
- Game sv FK. We use char(8) so we end up with just 8 padded spaces which obviously doesnt connect to anyone
  - this is probably the case for a decent number of FK's. Going to set to None if empty string. in parsing/parse_info_line
