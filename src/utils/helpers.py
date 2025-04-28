from dateutil.relativedelta import relativedelta
import logging



def get_date_intervals(start_date,end_date,freq_in_days=1,min_days=0,
                                       start_hour=9,start_minute=15,
                                       end_hour=15,end_minute=30):
    
    """
    Get date intervals with market time.
    Args:
        start_date (datetime): Start date.
        end_date (datetime): End date.
        freq_in_days (int): Frequency in days.
        start_hour (int): Start hour.
        start_minute (int): Start minute.
        end_hour (int): End hour.
        end_minute (int): End minute.
    Returns:
        list: List of date intervals.
    """

    logging.getLogger(__name__).debug(f"start_date: {start_date}, end_date: {end_date}, freq_in_days: {freq_in_days}")

    if start_date >= end_date or freq_in_days <= 0:
        return []
    if min_days <= 0 and start_date.date() == end_date.date():
        return []
    interval = []
    current_start = start_date
    while current_start < end_date:
        interval.append((
            current_start.replace(hour=start_hour,minute=start_minute),
            current_start.replace(hour=end_hour,minute=end_minute) + relativedelta(days=freq_in_days-1)
        ))
        current_start = interval[-1][1] + relativedelta(days=1)

    if interval[-1][1] > end_date:
        interval[-1] = (interval[-1][0], end_date.replace(hour=end_hour,minute=end_minute))
    logging.getLogger(__name__).debug(f"interval count: {len(interval)}")
    return interval

def print_header_line(console,columns):
    from rich.text import Text  # For colored text

    # --- Create the header string using a loop ---
    header_parts = []
    total_width = 0

    for column in columns:
        if hasattr(column, "_table_column"):  # Check if the column has a table_column attribute.
            header = Text(column._table_column.header, style=column._table_column.header_style, justify="left")  # Create a Text object with the header style
            header_width = column._table_column.width 
            if  column._table_column.header == "Task":
                header_width += 2
            if column._table_column.header == "Curr Value":
                header_width -= 1
            total_width += header_width #Accumulate header width values

            #Calculate the amount of padding the headers should take
            header.pad_right(header_width - len(column._table_column.header))  # Add padding
            header_parts.append(header)
        else: #There is no table column attribute, and skip append
            print("skipping the non column elements like progress")
    console.print("-" * total_width)  
    console.print(*header_parts)  # Print the header with the specified style
    console.print("-" * total_width)  


def load_config(config_path):
    """
    Load configuration from a JSON file, handling relative paths for sub-configs.

    Args:
        config_path (str): Path to the main JSON configuration file.

    Returns:
        dict: Parsed and merged configuration dictionary.
    """
    import json
    import os

    config_path = os.path.abspath(config_path) # Ensure main path is absolute
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    config_dir = os.path.dirname(config_path) # Get directory of the main config file

    with open(config_path, 'r') as file:
        main_config = json.load(file)

    merged_config = {}

    # Read all sub config files first
    for key, value in main_config.items():
        if "_config_file" in key:

            sub_config_path = os.path.abspath(os.path.join(config_dir, value))
            if not os.path.exists(sub_config_path):
                # Try absolute path if relative path fails
                sub_config_path = value
            if not os.path.exists(sub_config_path):
                raise FileNotFoundError(f"Sub-configuration file not found: {sub_config_path} (referenced in {config_path}). Current folder is {os.getcwd()}")
            
            with open(sub_config_path, 'r') as sub_file:
                sub_config = json.load(sub_file)
                merged_config.update(sub_config)

    # Now add/overwrite any other main config values
    for key, value in main_config.items():
        if "_config_file" not in key:  # Skip keys that are config files
            merged_config[key] = value

    return merged_config