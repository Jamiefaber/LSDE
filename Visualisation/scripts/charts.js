// initializes first graph
var data = {
    type: "sankey",
    orientation: "h",
    node: {
      pad: 15,
      thickness: 30,
      line: {
        color: "black",
        width: 0.5
      },
     label: first['label'],
     color: first['color']
        },
  
    link: {
      source: first['source'],
      target: first['target'],
      value:  first['count']
    }
  }
  
  var data = [data]
  
  var layout = {
    title: "Sankey Diagram of Flag States",
    font: {
      size: 12
    },
    margin:{
        t: 40,
        b: 0
    }
  }
Plotly.react('myDiv', data, layout, {scrollZoom: true});

// changes to selected graph, determines whether month day and date selected and plot graph with new values
function findplot(){
  var day = false;
  let eID = document.getElementById("plotselect");
  let plotVal = eID.options[eID.selectedIndex].value;
  if (plotVal >= 1 && plotVal <= 19) {
    dateslist = [first,g201702,g201703,g201704,g201705,g201706,g201707,g201708,g201602,g201603,g201604,g201605,g201606,g201607,g201608,g201609,g201610,g201611,g201612]
  }
  else{
    dateslist = [gd2,gd3,gd4,gd5,gd6,gd7,gd8,gd9,gd10,gd11,gd12,gd13,gd14,gd15,gd16,gd17,gd18,gd19,gd20,gd21,gd22,gd23,gd24,gd25,gd26,gd27,gd28,first] 
    day = true

  }
  for (let i = 0; i < 48; i++) {
    if (plotVal===String(i)){
      if (day == true){
        index = i - 20
        var temp = dateslist[index];
      }
      else{
        index = i - 1
        var temp = dateslist[index];
      }
    } 
  }

  var data = {
      type: "sankey",
      orientation: "h",
      node: {
        pad: 15,
        thickness: 30,
        line: {
          color: "red",
          width: 0.5
        },
        label: temp['label'],
        color: temp['color']
          },
    
      link: {
        source: temp['source'],
        target: temp['target'],
        value:  temp['count']
      }
    }   
  var data = [data]

  Plotly.react('myDiv', data, layout, {scrollZoom: true})
};