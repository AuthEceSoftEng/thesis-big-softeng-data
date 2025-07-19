// Live bar graph display

// Bar chart secondary title
var secondary_title = document.getElementById('secondaryTitle');

// Function to add the date of the last emitted (timestamp, number of events on timestamp)
// as the secondary title of the graph
function addNewDate(latestEventDateOnly) {
    secondary_title.innerHTML = `<p id="secondaryTitle" class="text-sm mb-0">
            <i class="fa fa-check text-info" aria-hidden="true"></i>
            <span class="font-weight-bold ms-1">Latest date of events available: </span>
            ${latestEventDateOnly}
            </p>`
}

// Bar chart bins
var ctx_live = document.getElementById("myBarChart_live").getContext("2d");
const maxNumberOfBinsToAppear = 7;
var myBarChart_live = new Chart(ctx_live, {
    type: "bar",
    data: {
        labels: [],
        datasets: [{
            label: 'Last acquired timestamp',
            backgroundColor: 'rgba(0, 0, 0, 255)',
            data: []
        }],
    },
    options: {
        borderWidth: 3,
        borderColor: ['rgba(0, 0, 0, 255)',],
    }
});


function addNewNumOfEvents(newNumberOfEvents, latestEventTimestamp, latestEventTimeOnly) {
    // Message to print in the HTML console
    // console.log('Got into function addNewNumOfEvents')
    myBarChart_live.data.labels.push(latestEventTimeOnly)
    myBarChart_live.data.datasets[0].data.push(newNumberOfEvents)
    myBarChart_live.data.datasets[0].label = latestEventTimestamp
    myBarChart_live.update();
}

function removeRedundantNumOfEvents() {
    // Message to print in the HTML console
    // console.log('Got into function removeRedundantNumOfEvents')
    if (myBarChart_live.data.labels.length > maxNumberOfBinsToAppear) {
        myBarChart_live.data.labels.splice(0, 1)
        myBarChart_live.data.datasets[0].data.splice(0, 1)
        myBarChart_live.update()
    }

}





// Socket to change the UI
var socket_live = io.connect();


// Update the number of near real time events in the shifting bar graph
socket_live.on("updateNumOfNearRealTimeRawEvents", function (msg) {
    // // Debug the socket_live bar graph activity. If the log prints the messages, 
    // // it works as expected.
    // console.log("Received a new number of raw events: timestamp" + msg.timestamp +
    //     ", events" + msg.num_of_events_per_sec);

    num_of_events_per_sec = msg.num_of_events_per_sec

    // Turn '2024-08-21T23:05:06Z' to '2024-08-21 23:05:06'
    let readable_timestamp = msg.timestamp.replace('T', ' ').replace('Z', '')
    
    let timestamp_with_text = 'Number of events on timestamp'

    // Turn '2024-08-21T23:05:06Z' to '23:05:06'
    let latestEventTimeOnly = new Date(msg.timestamp).toISOString().substring(11, 19);

    addNewNumOfEvents(num_of_events_per_sec, timestamp_with_text, latestEventTimeOnly)

    removeRedundantNumOfEvents()

    let latestEventDateOnly = new Date(msg.timestamp).toISOString().substring(0, 10);

    addNewDate(latestEventDateOnly)
})









