import os
import glob
from parse import parse
import pandas as pd
import cx_Oracle

def create_or_update_lookup_file(directory, lookup_file):
    file_list = os.listdir(directory)

    with open(lookup_file, 'a+') as file:
        # Read existing entries in the lookup file
        existing_files = {line.strip() for line in file}

        # Find new files that are not in the lookup file
        new_files = [file for file in file_list if file not in existing_files]

        # Create an empty DataFrame to store the data
        combined_df = pd.DataFrame()

        # Process new files
        for new_file in new_files:
            file_path = os.path.join(directory, new_file)

            # Read the content of the new file
            with open(file_path, 'r') as handle:
                data = handle.readlines()

            fmt = '{host} {identd} {userid:d} [{time:th}] "{request}" {status:d} {size:d} {referrer} {user_agent}'
            parsed_data = map(lambda x: parse(fmt, x).named, data)

            # Create a DataFrame from the parsed data
            df = pd.DataFrame(parsed_data)

            # Create a new column for the insert timestamp
            df['insert_timestamp'] = pd.to_datetime('now', utc=True)

            # Concatenate the current DataFrame with the combined DataFrame
            combined_df = pd.concat([combined_df, df], ignore_index=True)

            # Process the DataFrame as needed (e.g., print, save to database, etc.)
            print(f"Processing file: {new_file}")

            # Update the lookup file with the new file
            file.write("%s\n" % new_file)

        return combined_df

def load_data_to_oracle(dataframe, table_name, connection_string):
    connection = cx_Oracle.connect(connection_string)

    # Check if the table exists
    cursor = connection.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM user_tables WHERE table_name = '{table_name.upper()}'")
    table_exists = cursor.fetchone()[0] == 1

    if not table_exists:
        # Create the table if it doesn't exist
        cursor.execute(f"CREATE TABLE {table_name} ("
                       "HOST VARCHAR2(255), "
                       "IDENTD VARCHAR2(255), "
                       "USERID NUMBER, "
                       "LOG_TIME TIMESTAMP, "
                       "REQUEST VARCHAR2(4000), "
                       "STATUS NUMBER, "
                       "SIZE NUMBER, "
                       "REFERRER VARCHAR2(4000), "
                       "USER_AGENT VARCHAR2(4000), "
                       "INSERT_TIMESTAMP TIMESTAMP)"
                       )

    # Load only the data not already in the table (assuming 'LOG_TIME' is the timestamp column)
    existing_data = pd.read_sql(f"SELECT DISTINCT LOG_TIME FROM {table_name}", connection)
    new_data = dataframe[~dataframe['LOG_TIME'].isin(existing_data['LOG_TIME'])]

    # Insert new data into the Oracle table
    new_data.to_sql(table_name, connection, index=False, if_exists='append', method='multi')

    connection.commit()
    connection.close()

# Example usage
directory_path = 'Path_of_Existing_Log_files/logs/'
lookup_file_path = 'Path_of_lookup_File/lookup_file.txt'
oracle_table_name = 'LOG_TABLE'
oracle_connection_string = 'your_username/your_password@your_oracle_host:your_oracle_port/your_service_name'

# Create or update the lookup file with the current list of files in the directory
result_df = create_or_update_lookup_file(directory_path, lookup_file_path)

# Load data into Oracle table
load_data_to_oracle(result_df, oracle_table_name, oracle_connection_string)

# Print or further process the combined DataFrame
print(result_df)
