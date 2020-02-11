var data = [{
        x: ['VALUE 1'], // in reality I have more values...
        y: [20],
        type: 'bar'
    }];
    Plotly.newPlot('PlotlyTest', data);
    
    function adjustValue1(value)
    {
        data[0]['y'][0] = value;
	console.log(value)
        Plotly.redraw('PlotlyTest');
    }
