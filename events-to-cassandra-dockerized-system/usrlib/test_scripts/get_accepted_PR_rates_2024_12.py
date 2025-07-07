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


    # Get accepted PR rates from cassandra for each file

    day_datetime = datetime(year=2024, month=12, day=1)
    day_string = datetime.strftime(day_datetime, "%Y-%m-%d")
    print(f"On day: {day_string}")


    def get_PR_rates_of_day(cassandra_keyspace, session, day_string):
        
        # Human PRs
        #region 
        number_of_prs_table = "number_of_pull_requests_by_humans"
        
        were_accepted = True
        select_PR_in_day = f'SELECT number_of_pull_requests FROM \
        {cassandra_keyspace}.{number_of_prs_table} WHERE day = ? \
        AND were_accepted = ?;'
        select_PR_in_day_prepared = session.prepare(select_PR_in_day)
        rows = session.execute(select_PR_in_day_prepared, [day_string, were_accepted])    
        # PRs may not exist for the day
        if rows.one() != None:
            accepted_human_PRs = getattr(rows.one(), 'number_of_pull_requests')
        else:
            accepted_human_PRs = 0
        
        were_accepted = False
        select_PR_in_day = f'SELECT number_of_pull_requests FROM \
        {cassandra_keyspace}.{number_of_prs_table} WHERE day = ? \
        AND were_accepted = ?;'
        select_PR_in_day_prepared = session.prepare(select_PR_in_day)
        rows = session.execute(select_PR_in_day_prepared, [day_string, were_accepted]) 
        if rows.one() != None:   
            rejected_human_PRs = getattr(rows.one(), 'number_of_pull_requests')
        else:
            rejected_human_PRs = 0
        # endregion
        
        # Bot PRs
        #region 
        number_of_prs_table = "number_of_pull_requests_by_bots"
        
        were_accepted = True
        select_PR_in_day = f'SELECT number_of_pull_requests FROM \
        {cassandra_keyspace}.{number_of_prs_table} WHERE day = ? \
        AND were_accepted = ?;'
        select_PR_in_day_prepared = session.prepare(select_PR_in_day)
        rows = session.execute(select_PR_in_day_prepared, [day_string, were_accepted])    
        if rows.one() != None:   
            accepted_bot_PRs = getattr(rows.one(), 'number_of_pull_requests')
        else:
            accepted_bot_PRs = 0
        
        were_accepted = False
        select_PR_in_day = f'SELECT number_of_pull_requests FROM \
        {cassandra_keyspace}.{number_of_prs_table} WHERE day = ? \
        AND were_accepted = ?;'
        select_PR_in_day_prepared = session.prepare(select_PR_in_day)
        rows = session.execute(select_PR_in_day_prepared, [day_string, were_accepted])    
        if rows.one() != None:   
            rejected_bot_PRs = getattr(rows.one(), 'number_of_pull_requests')
        else:
            rejected_bot_PRs = 0
        # endregion
        
        
        return accepted_human_PRs, rejected_human_PRs, accepted_bot_PRs, rejected_bot_PRs
        
    
    starting_datetime = datetime(year=2024, month=12, day=1)
    ending_datetime = datetime(year=2024, month=12, day=31)
    
    
    avg_accepted_PR_rates_files = \
        "/test_scripts/accepted_PR_rates_2024_12.txt"
    with open(avg_accepted_PR_rates_files, 'a') as file_obj:
        file_obj.write("Day\t\tAccepted human PRs\t\tRejected human PRs\t\t"
                       "Accepted bot PRs\t\tRejected bot PRs\n")
        
        total_accepted_human_PRs = 0
        total_rejected_human_PRs = 0
        total_accepted_bot_PRs = 0
        total_rejected_bot_PRs = 0
        
        current_datetime = starting_datetime
        while current_datetime <= ending_datetime:
            current_day_string = datetime.strftime(current_datetime, "%Y-%m-%d")
            accepted_human_PRs, rejected_human_PRs, \
                accepted_bot_PRs, rejected_bot_PRs = \
                    get_PR_rates_of_day(cassandra_keyspace, \
                    session, current_day_string)
            
            
            
            
            print(current_day_string, 
                accepted_human_PRs, rejected_human_PRs,
                accepted_bot_PRs, rejected_bot_PRs)
            line_formatted = f"{current_day_string}\t\t{accepted_human_PRs}\t\t"\
                f"{rejected_human_PRs}\t\t"\
                f"{accepted_bot_PRs}\t\t{rejected_bot_PRs}\n"
                
            file_obj.write(line_formatted)
            
            
            
            total_accepted_human_PRs += accepted_human_PRs
            total_rejected_human_PRs += rejected_human_PRs
            total_accepted_bot_PRs += accepted_bot_PRs
            total_rejected_bot_PRs += rejected_bot_PRs
            
            current_datetime += timedelta(days=1)
    
        file_obj.write("-------------------------------------------------------------------\n")
        
        file_obj.write("Total human accepted\t\tTotal human rejected\t\tTotal human PRs\t\t"
                       "Human accepted rate\n")
        total_human_PRs = total_accepted_human_PRs+total_rejected_human_PRs
        avg_accepted_human_PRs = round((total_accepted_human_PRs/total_human_PRs)*100, 2)
        line_formatted = f"{total_accepted_human_PRs}\t\t{total_rejected_human_PRs}\t\t"\
                f"{total_human_PRs}\t\t{avg_accepted_human_PRs}%\n"        
        file_obj.write(line_formatted)
        
        
        file_obj.write("-------------------------------------------------------------------\n")
        file_obj.write("Total bot accepted\t\tTotal bot rejected\t\tRejected bot PRs\t\t"
                       "Bot accepted rate\n")
        total_bot_PRs = total_accepted_bot_PRs+total_rejected_bot_PRs
        avg_accepted_bot_PRs = round((total_accepted_bot_PRs/total_bot_PRs)*100, 2)
        line_formatted = f"{total_accepted_bot_PRs}\t\t{total_rejected_bot_PRs}\t\t"\
                f"{total_bot_PRs}\t\t{avg_accepted_bot_PRs}%\n"        
        file_obj.write(line_formatted)
        
    

    
    # For one day, get the accepted, rejected humans and bots number of pull requests and print it using a function


    # Access file
    # Write into file the avg accepted PR rates for all files and days
    # Replicate the behaviour for the number of events case