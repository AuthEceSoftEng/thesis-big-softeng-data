// List refreshing on star/fork
const list = document.getElementById('list');
const maxItems = 20;

function insertNewEventInList(username, event_type, repo_name, timestamp) {
    let eventText;
    eventText = document.createElement('li');
    if (event_type == 'WatchEvent') {
        // eventText.textContent = `${username} starred ${repo_name}`;
        eventText.innerHTML = `<span class="font-weight-bold ms-1">${username}</span>` + ` starred ` +
          `<span class="font-weight-bold ms-1">${repo_name}</span>`;
    } else if (event_type == 'ForkEvent') {
        // eventText.textContent = `${username} forked ${repo_name}`;
        eventText.innerHTML = `<span class="font-weight-bold ms-1">${username}</span>` + ` forked ` +
          `<span class="font-weight-bold ms-1">${repo_name}</span>`;
    } else {
        eventText.textContent = `Waiting for the latest star and fork events`;
    }

    list.appendChild(eventText);

    if (list.children.length > maxItems) {
        list.removeChild(list.firstElementChild);
    }
}

// function insertNewEventInList(username, event_type, repo_name, timestamp) {
//     let newEventInList = document.createElement('li');
//     let eventText;

//     if (event_type == 'WatchEvent') {
//         eventText = `${username} starred ${repo_name}`;
//     } else if (event_type == 'ForkEvent') {
//         eventText = `${username} forked ${repo_name}`;
//     } else {
//         eventText = `Waiting for the latest star and fork events`;
//     }

//     newEventInList.innerHTML = `<span class="font-weight-bold ms-1">${eventText}</span>`;
//     list.appendChild(newEventInList);

//     if (list.children.length > maxItems) {
//         list.removeChild(list.firstElementChild);
//     }
// }

// Socket to change the UI
var socket = io.connect();

// Update the star and fork events shifting list 
socket.on("updateNearRealTimeStarsForks", function (msg) {
    // // Console log for debugging purposes (see if it is printed in 
    // // the Console tab of the browser) 
    // console.log("Received new star or fork event :: " + msg.value);
    insertNewEventInList(msg.username, msg.event_type, msg.repo_name,
        msg.timestamp);
});