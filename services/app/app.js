var mymap = L.map('map').setView([52.23, 21], 11);
L.tileLayer('https://api.mapbox.com/styles/v1/{id}/tiles/{z}/{x}/{y}?access_token={accessToken}', {
    attribution: 'Map data &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, Imagery © <a href="https://www.mapbox.com/">Mapbox</a>',
    maxZoom: 18,
    id: 'mapbox/streets-v11',
    tileSize: 512,
    zoomOffset: -1,
    accessToken: 'pk.eyJ1IjoicG9uZXdvciIsImEiOiJjazlxMWEzYzcwZDFsM2R0Z2Zobmk3eDZyIn0.wgcT2EVmIpu1Rc5suPcaeg'
}).addTo(mymap);

var busIcon = L.icon({
    iconUrl: 'bus.svg',
    shadowUrl: 'bus.svg',

    iconSize:     [38, 95], // size of the icon
    shadowSize:   [0, 0], // size of the shadow
    iconAnchor:   [22, 40], // point of the icon which will correspond to marker's location
    shadowAnchor: [4, 62],  // the same for the shadow
    popupAnchor:  [-3, -40] // point from which the popup should open relative to the iconAnchor
});


document.querySelector('form').addEventListener('submit', handleSubmitForm);

let markers = [];

let stop = '';

var drawMarkers = function () {
    if (stop === '') {
        return;
    }
    fetch('http://localhost:8080/stop/' + stop)
        .then(response => response.json())
        .then(
            data => function () {
                removeMarkers();
                data.forEach(
                    el => function () {
                        let marker = L.marker([el.lat, el.lon], {icon: busIcon, rotationAngle: el.bearing - 90}).addTo(mymap);
                        marker.bindPopup('<b>' + el.lines + '</b>');
                        markers.push(marker);
                    }()
                );
            }()
        );
}

var sleep = time => new Promise(resolve => setTimeout(resolve, time));
var poll = (promiseFn, time) => promiseFn().then(sleep(time).then(() => poll(promiseFn, time)));

poll(() => new Promise(drawMarkers), 2000);

function removeMarkers() {
    for (var i = 0; i < markers.length; i++) {
        mymap.removeLayer(markers[i]);
    }
    markers = [];
}

function handleSubmitForm(e) {
    e.preventDefault();
    let input = document.querySelector('input');
    if (input.value !== '') {
        stop = input.value;
    }
}
