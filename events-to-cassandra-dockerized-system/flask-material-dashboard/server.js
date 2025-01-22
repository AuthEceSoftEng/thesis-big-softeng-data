/*
Deploys a server on localhost:3000 and exposes the data returned by the
query in endpoint localhost:3000/data
Used paired with
/home/xeon/Thesis/flask-material-dashboard/apps/templates/accounts/login.html
to fetch the data and expose it in the app
*/

const express = require('express');
const cors = require('cors');
const cassandra = require('cassandra-driver');
const app = express();
const client = new cassandra.Client({
    contactPoints: ['127.0.0.1:9042'],
    localDataCenter: 'datacenter1',
    keyspace: 'mykeyspace',
});

app.use(cors());
app.get('/stats_by_day', async (req, res) => {
    const result = await client.execute('SELECT * FROM mykeyspace.repos LIMIT 5');
    res.json(result.rows);
});

app.listen(3000, () => console.log('Server running on port 3000'));


