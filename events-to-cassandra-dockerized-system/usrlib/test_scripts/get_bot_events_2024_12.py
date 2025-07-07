from cassandra.cluster import Cluster
from datetime import datetime, timedelta
import sys

# Write outline of code here and make testing


if __name__ == '__main__':
    # Connect to cassandra
    cassandra_host = 'cassandra_stelios'
    cassandra_port = 9142
    cassandra_keyspace = 'prod_gharchive'
    cluster = Cluster([cassandra_host], port=cassandra_port, connect_timeout=10)
    session = cluster.connect(cassandra_keyspace, wait_for_all_pools=True)
    session.execute(f'USE {cassandra_keyspace}')


    def get_bot_events_on_day(cassandra_keyspace, session, day_string):
        
        # Humans
        number_of_events_per_type = []
        total_number_of_human_events_on_day = 0
        prepared_query = f"" \
            f"SELECT number_of_events, event_type FROM {cassandra_keyspace}.number_of_human_events_per_type_by_day "\
            f"WHERE day = '{day_string}';"
        
        
        rows = session.execute(prepared_query)
        
        for row in rows:
            number_of_events_per_type.\
                append({col_name: getattr(row, col_name) for col_name in row._fields})
            total_number_of_human_events_on_day += row.number_of_events
        
        
        # Bots
        number_of_events_per_type = []
        total_number_of_bot_events_on_day = 0
        
        prepared_query = f""\
            f"SELECT number_of_events, event_type FROM {cassandra_keyspace}.number_of_bot_events_per_type_by_day "\
            f"WHERE day = '{day_string}';"
        rows = session.execute(prepared_query)
        for row in rows:
            number_of_events_per_type.\
                append({col_name: getattr(row, col_name) for col_name in row._fields})
            total_number_of_bot_events_on_day += row.number_of_events
        
        
        
        
         
        
        
        return total_number_of_human_events_on_day, total_number_of_bot_events_on_day
    

    
    starting_datetime = datetime(year=2024, month=12, day=1)
    ending_datetime = datetime(year=2024, month=12, day=12)
    
    days_counter = 0
    bot_events_filepath = '/test_scripts/bot_events_2024_12.txt'
    with open(bot_events_filepath, 'a') as file_obj:
        file_obj.write("Day\t\t\tHuman events\t\t\tBot events\n")
    
        human_events_for_all_days = 0
        bot_events_for_all_days = 0
        
        current_datetime = starting_datetime
        while current_datetime <= ending_datetime:
            days_counter += 1
            current_day_string = datetime.strftime(current_datetime, "%Y-%m-%d")
            
            number_of_human_events_on_day, number_of_bot_events_on_day = get_bot_events_on_day(cassandra_keyspace, session, current_day_string)
            
            human_events_for_all_days += number_of_human_events_on_day
            bot_events_for_all_days += number_of_bot_events_on_day
            
            line_formatted = f"{current_day_string}\t\t{number_of_human_events_on_day}\t\t{number_of_bot_events_on_day}\n"
            print(line_formatted)
            file_obj.write(line_formatted)
            current_datetime += timedelta(days=1)
    
        file_obj.write("-----------------------------------\n")
        file_obj.write(f"Total events\t\tTotal human events\t\tTotal bot events\n")
        file_obj.write(f"{human_events_for_all_days+bot_events_for_all_days}\t\t{human_events_for_all_days}\t\t{bot_events_for_all_days}\n")
        
        file_obj.write("-----------------------------------\n")
        file_obj.write(f"Average human events per file\t\tAverage bot events per file\n")
        avg_human_events = round(human_events_for_all_days/days_counter)
        avg_bot_events = round(bot_events_for_all_days/days_counter)
        file_obj.write(f"{avg_human_events}\t\t{avg_bot_events}\n")
        
        file_obj.write("-----------------------------------\n")
        file_obj.write(f"Percentage human events\t\tPercentage bot events\n")
        total_events_for_all_days = human_events_for_all_days+bot_events_for_all_days
        human_events_percentage = round(human_events_for_all_days/total_events_for_all_days*100, 2)
        bot_events_percentage = round(bot_events_for_all_days/total_events_for_all_days*100, 2)
        file_obj.write(f"{human_events_percentage}%\t\t{bot_events_percentage}%\n")
        
    cluster.shutdown()