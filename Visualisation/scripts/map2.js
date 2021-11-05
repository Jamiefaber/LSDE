var sliderControl = null;
// initialize map
var mymap = L.map('mapid').setView([25, 0], 2);
var tile = L.tileLayer('https://api.mapbox.com/styles/v1/{id}/tiles/{z}/{x}/{y}?access_token={accessToken}', {
    attribution: 'Map data &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, Imagery © <a href="https://www.mapbox.com/">Mapbox</a>',
    maxZoom: 7, minZoom: 2, id: 'mapbox/streets-v11', tileSize: 512, zoomOffset: -1, accessToken: 'pk.eyJ1IjoiamFtaWVmYWJlciIsImEiOiJja3VvN2ltN28xMHRuMm9wZm5mMW0yb3V5In0.GfKC-79fT5QXd4sGT1x2nw'
}).addTo(mymap);

// initialize markers and customizes markers
var newIcon = L.icon({
    iconUrl: 'static/img/ship.png',
    iconSize:     [55, 40], // size of the icon
    iconAnchor:   [10, 20], // point of the icon which will correspond to marker's location
    popupAnchor:  [0, 0]
});
var markerscluster = L.markerClusterGroup();
L.Marker.mergeOptions({
    icon: newIcon
});

// loads in geojson data
var geoJsonLayer = L.geoJson(encounters, {
    onEachFeature: function (feature, layer) {
        var content = "<div style='clear: both'></div><div><CAPTION><h6>Meeting Info</h6></CAPTION><table> <tr><td>Meeting Coordinates</td><td>(" + feature.properties.Latitude.toFixed(1)+ ", " +feature.properties.Longitude.toFixed(1) + ")</td></tr><tr><td>Vessel Flag State</td><td>" + feature.properties.Countries + "</td></tr><tr><td>Duration Time</td><td>" + feature.properties.Duration.toFixed(1) + "h</td></tr></table></div>"
        layer.bindPopup(content);
    }
});

// clusters markers in layer
L.markerClusterGroup.layerSupport({chunkedLoading:true}).addTo(mymap).checkIn(geoJsonLayer);

var sliderControl = L.control.sliderControl({
    position: "topright",
    layer: geoJsonLayer,
    range: false,
    follow: true,
    alwaysShowDate: true
});

// add slider to map and initializes
mymap.addControl(sliderControl);
sliderControl.startSlider();