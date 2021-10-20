var labels = waterpolygon.features.map(function(e) {
    return e.properties.NAME;
    });
    var data = waterpolygon.features.map(function(e) {
    return e.properties.ID;
    });;

    var ctx = canvas.getContext('2d');
    var config = {
    type: 'line',
    data: {
        labels: labels,
        datasets: [{
            label: 'Graph Line',
            data: data,
            backgroundColor: 'rgba(0, 119, 204, 0.3)'
        }]
    }
    };

    var chart = new Chart(ctx, config);