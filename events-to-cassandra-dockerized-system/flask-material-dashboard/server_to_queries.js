/*
Deploys a server on localhost:3000 and exposes the data returned by the
query in endpoint localhost:3000/data
Used paired with
/home/xeon/Thesis/flask-material-dashboard/apps/templates/accounts/login.html
to fetch the data and expose it in the app
*/

// const express = require('express');
// const cors = require('cors');
// const cassandra = require('cassandra-driver');
// const app = express();
// // const client = new cassandra.Client({
// //     contactPoints: ['127.0.0.1:9142'],
// //     localDataCenter: 'datacenter1',
// //     keyspace: 'mykeyspace',
// // });

// app.use(cors());
// app.get('/stats_by_day', async (req, res) => {
//     const result = await client.execute('SELECT * FROM mykeyspace.repos LIMIT 5');
//     res.json(result.rows);
// });

// app.listen(3000, () => console.log('Server running on port 3000'));




// i need to make a function in js that has the same functionality as 
// query_distinct_results() from file 
// /home/xeon/Thesis/local-kafka-flink-cassandra-integration/presentation-10-demo/task-2-store-tables-repos-and-stats/query_cassandra.py



const cassandra = require('cassandra-driver');
const client = new cassandra.Client({
    contactPoints: ['127.0.0.1:9142'],
    localDataCenter: 'datacenter1',
    keyspace: 'mykeyspace',
});


// async function queryCassandra() {
// console.log("Hi");
// const result = await client.execute('SELECT * FROM mykeyspace.repos LIMIT 5');
// var rowsListLength = result.rows.length;
// for (var rowIndex; rowIndex < rowsListLength; rowIndex++) {

// } 

//  Get the field index returned by the query corresponding to the 
//  `distinct_field_name`
async function get_result_fields() {

    const topic_select_query = " \
    SELECT * FROM gharchive.popular_topics_by_day \
        WHERE day = '2024-09-09' \
        ORDER BY number_of_events DESC LIMIT 1; \
    "
    
    const distinct_field_name = 'topic'
    const res_result_set_to_get_fields = 
        await client.execute(topic_select_query.split('LIMIT')[0] + "LIMIT 1");    
    const distinct_field_index_in_row = 
        res_result_set_to_get_fields.rows[0].number_of_events;

        // ._fields.index(distinct_field_name)
        
    const field_name = "number_of_events";
    console.log(res_result_set_to_get_fields.rows[0][field_name]);

    // # List with distinct values of the field `distinct_field_name` 
    var distinct_field_list = []
    // # Rows returned with the distinct values for the field `distinct_field_name`
    var distinct_row_list = []
        
    // // TODO: Insert while statement here

    // // # Query on the new query limits
    // select_query = select_query.split('LIMIT')[0] + "LIMIT" + query_limit + ";"
    // res_result_set = await client.execute(select_query);
    // // # List with the new resulting rows 
    // res_rows_list = res_result_set.all()/
    
    const number_of_results = 2;

    // Iterate over all rows to find the distinct ones
    for (var res_row_index = 0; res_row_index < res_rows_list.length;
        res_row_index++) {
        let res_row_field_value = res_row_list[res_row_index];

        // Iterate through the list to check if the value is there (if it not,
        //  add it in the list)
        for (let distinct_field_index = 0;
            distinct_field_index < distinct_field_list.length;
            distinct_field_index++) {
            
            // If the field value does not exist, add it to the array 
            if (distinct_field_list[distinct_field_index] != res_row_field_value) {
                distinct_field_list.push(res_row_field_value)
                distinct_row_list.push(res_row)
                if (distinct_field_list.length == number_of_results){
                    break
                }
                    
            }

        }
    }


    // console.log(distinct_field_index_in_row);
    
    client.shutdown()
}

get_result_fields()

// const distinct_field_index_in_row = 
//     res_result_set_to_get_fields.one()._fields.index(distinct_field_name)


// distinct_field_index_in_row
// # List with distinct values of the field `distinct_field_name` 
// distinct_field_list = []
// # Rows returned with the distinct values for the field `distinct_field_name`
// distinct_row_list = []



// client.shutdown()
// };


// queryCassandra();