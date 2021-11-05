// initialize map
var map = L.map('map').setView([25, 0], 2);
var tile = L.tileLayer('https://api.mapbox.com/styles/v1/{id}/tiles/{z}/{x}/{y}?access_token={accessToken}', {
    attribution: 'Map data &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, Imagery © <a href="https://www.mapbox.com/">Mapbox</a>',
    maxZoom: 7, minZoom: 2, id: 'mapbox/streets-v11', tileSize: 512, zoomOffset: -1, accessToken: 'pk.eyJ1IjoiamFtaWVmYWJlciIsImEiOiJja3VvN2ltN28xMHRuMm9wZm5mMW0yb3V5In0.GfKC-79fT5QXd4sGT1x2nw'
}).addTo(map);

// highlights polygon when mouse is hovering
function highlightFeature(e) {
    var layer = e.target;
    layer.setStyle({
        weight: 5,
        opacity: 0.7,
        color: '#666',
        dashArray: '',
        fillOpacity: 0.01
    });
    if (!L.Browser.ie && !L.Browser.opera && !L.Browser.edge) {
        layer.bringToFront();
    }
}

// resets highlight once mouse isn't hovering 
function resetHighlight(e) {
    var layer = e.target;

    layer.setStyle({
    weight: 3,
    opacity: 0.1,
    color: 'blue',
    dashArray: '',
    fillOpacity: 0.01
    });
}

// attributes colour to markers according to quantity
function getColor(number) {
    if (number < 20){
        return 'green'
    }
    if (number < 250){
        return 'darkgreen'
    }
    if (number < 1000){
        return 'orange'
    }
    if (number < 2500){
        return 'red'
    }
    else{
        return 'darkred'
    }
}

function onClick(e) {
    e.target.getPopup();       
}

// draws polygons on map
for (var i = -180; i < 180; i+=10) {
    for (var j = -90; j < 90; j+=10) {
        var grid = [[j+10, i+10],[j, i+10],[j, i],[j+10, i]]
        var polygon = L.polygon(grid).addTo(map);
        polygon.setStyle({weight: 3, opacity: 0.1, color: 'blue', dashArray: '', fillOpacity: 0.01})
        polygon.on('mouseover', highlightFeature);
        polygon.on('mouseout', resetHighlight);
        }
    } 

var markers = new Array();
var markersarray = [];
var counter = 1;

// loads months of 2017 and makes markers
var mark = L.geoJson(results,{
    onEachFeature: function (feature, layer) {   
        var marker = L.marker(feature.geometry.coordinates, {
                icon: new L.AwesomeNumberMarkers({
                    number: feature.properties.Encounters,
                    markerColor: getColor(Number(feature.properties.Encounters)),
                    }),
                    })
        var content = "<div style='clear: both'></div><div><CAPTION><h6>Grid Cell Info</h6></CAPTION><table> <tr><td>Midpoint Coordinates: </td><td>(" + feature.geometry.coordinates + ")</td></tr><tr><td>Meeting Amount: </td><td>" + feature.properties.Encounters + "</td></tr><tr><td>Average Duration: </td><td>" + feature.properties.Duration.toFixed(1) + "h</td></tr><tr><td>Number Days in Layer: </td><td>" + feature.properties.days + "</td></tr></table></div>";
        marker.bindPopup(content, {closeButton: true});
        marker.on('click', onClick);
        if (feature.properties.Month != counter){
            markersarray.push(markers)
            counter += 1
            markers = []
        }
        markers.push(marker)
}})
markers.push
markersarray.push(markers);

var markers = new Array();
var counter = 1;
var markersarray16 = [];

// loads months of 2016 in and makes markers
var mark = L.geoJson(results16,{
    onEachFeature: function (feature, layer) {   
        var marker = L.marker(feature.geometry.coordinates, {
                icon: new L.AwesomeNumberMarkers({
                    number: feature.properties.Encounters,
                    markerColor: getColor(Number(feature.properties.Encounters)),
                    }),
                    })
        var content = "<div style='clear: both'></div><div><CAPTION><h6>Grid Cell Info</h6></CAPTION><table> <tr><td>Midpoint Coordinates: </td><td>(" + feature.geometry.coordinates + ")</td></tr><tr><td>Meeting Amount: </td><td>" + feature.properties.Encounters + "</td></tr><tr><td>Average Duration: </td><td>" + feature.properties.Duration.toFixed(1) + "h</td></tr><tr><td>Number Days in Layer: </td><td>" + feature.properties.days + "</td></tr></table></div>";
        marker.bindPopup(content, {closeButton: true});
        marker.on('click', onClick);
        if (feature.properties.Month != counter){
            markersarray16.push(markers)
            counter += 1
            markers = []
        }
        markers.push(marker)
}})
markers.push
markersarray16.push(markers);

// creates layers with all the markers
var dates = ['January','February','March','April','May','June','July','August','September','October','November','December']
var overlayMaps = {};
for (var i = 1; i < markersarray.length+1; i++) {
    if (i == 1){
        overlayMaps[dates[i-1]] = L.layerGroup(markersarray[i-1]).addTo(map);
    }else{
        overlayMaps[dates[i-1]] = L.layerGroup(markersarray[i-1]);
    }
} 
var overlayMaps16 = {};
for (var i = 2; i < markersarray16.length+1; i++) {
    if (i == 1){
        overlayMaps16[dates[i-1]] = L.layerGroup(markersarray16[i-1]).addTo(map);
    }else{
        overlayMaps16[dates[i-1]] = L.layerGroup(markersarray16[i-1]);
    }
}

// layer format for control panel
var overlays = [
    {
        groupName : "2016",
        expanded  : false,
        layers    : overlayMaps16
    },
    {
        groupName : "2017",
        expanded  : false,
        layers    : overlayMaps
    }
];

var options = {
    container_width 	: "300px",
    container_maxHeight : "350px", 
    group_maxHeight     : "80px",
    exclusive       	: true
};

// adds control to map with overlays
var control = L.Control.styledLayerControl(overlays,options);
map.addControl(control);