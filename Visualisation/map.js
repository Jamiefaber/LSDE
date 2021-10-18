// js map in leaflet here, do i need this file?
var sliderControl = null;
var geoJsonLayer;
let [arrayDG,arrayG,arrayO,arrayR,arrayDR] = [[], [], [], [], []]
        // initialize map
        var mymap = L.map('mapid', { zoomControl: false }).setView([0, 0], 2);
        L.tileLayer('https://api.mapbox.com/styles/v1/{id}/tiles/{z}/{x}/{y}?access_token={accessToken}', {
            attribution: 'Map data &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, Imagery © <a href="https://www.mapbox.com/">Mapbox</a>',
            maxZoom: 9, id: 'mapbox/streets-v11', tileSize: 512, zoomOffset: -1, accessToken: 'pk.eyJ1IjoiamFtaWVmYWJlciIsImEiOiJja3VvN2ltN28xMHRuMm9wZm5mMW0yb3V5In0.GfKC-79fT5QXd4sGT1x2nw'
        }).addTo(mymap);
        function style(feature) {
        return {
            weight: 3,
            opacity: 0.2,
            color: 'blue',
            dashArray: '',
            fillOpacity: 0.01
            };
        }
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

        function resetHighlight(e) {
            var layer = e.target;

            layer.setStyle({
            weight: 3,
            opacity: 0.2,
            color: 'blue',
            dashArray: '',
            fillOpacity: 0.01
            });
        }

        function getColor(number) {
            if (number < 10){
                return 'darkgreen'
            }
            if (number < 20){
                return 'green'
            }
            if (number < 50){
                return 'orange'
            }
            if (number < 75){
                return 'red'
            }
            else{
                return 'darkred'
            }
        }

        function onClick(e) {
            var popup = e.target.getPopup();
            var content = popup.getContent();

            console.log(content);
        
        }

        function createLayers(number, marker){
            if (number < 10){
                arrayDG.push(marker)
            }
            if (number < 20){
                arrayG.push(marker)
            }
            if (number < 50){
                arrayO.push(marker)
            }
            if (number < 75){
                arrayR.push(marker)
            }
            else{
                arrayDR.push(marker)
            }
        }
    // load in grid as polygons
        var geoJsonLayer = L.geoJson(waterpolygon, {style: style,
            onEachFeature: function (feature, layer) {
                layer.on({
                mouseover: highlightFeature,
                mouseout: resetHighlight
                });
                // Get bounds of polygon
                var bounds = layer.getBounds();
                // Get center of bounds
                var center = bounds.getCenter();
                // Use center to put marker on map
                var marker = L.marker(center, {
                icon: new L.AwesomeNumberMarkers({
                    number: feature.properties.ID,
                    markerColor: getColor(Number(feature.properties.ID)),
                    }),
                }).addTo(mymap);
                marker.bindPopup('Name: ' + feature.properties.NAME);
                marker.on('click', onClick);
                createLayers(feature.properties.ID, marker)
            }
        }).addTo(mymap);
        var layers = L.layerGroup(arrayDG)
        sliderControl = L.control.sliderControl({
            position: "topright",
            layer: layers,
            follow: 3
            });
        mymap.addControl(sliderControl);
        sliderControl.startSlider();
        mymap.touchZoom.disable();
        mymap.doubleClickZoom.disable();
        mymap.scrollWheelZoom.disable();
        mymap.boxZoom.disable();
        mymap.keyboard.disable();