// Set the dimensions and margins of the graph
const margin = { top: 30, right: 15, bottom: 100, left: 100 },
  width = $('#graph').innerWidth() * 0.7,
  height = window.innerHeight * 0.7;

// append the svg object to the body of the page
const svg = d3
  .select('#graph')
  .append('svg')
  .attr('width', width + margin.left + margin.right)
  .attr('height', height + margin.top + margin.bottom)
  .append('g')
  .style('transform', `translate(${margin.left}px, ${margin.top}px)`);

svg.append('g').attr('class', 'x-axis');
svg.append('g').attr('class', 'y-axis');

$(document).ready(function () {
  // Initialise the graph with the default metric
  var defaultMetric = $('#changeHistMetric option:selected').val();
  update(defaultMetric);

  $('.copo-main').removeClass('copo-main');

  // Handle changes in the selected metric
  $(document).on('change', '#changeHistMetric', function () {
    var val = $(this).children('option:selected').val();
    update(val);
  });

  // Set active tab
  if (window.location.pathname.includes('histogram')) {
    // Remove class from .ul_style li
    $('.ul_style li').removeClass('active');
    // Add class to the one clicked
    $('#histogramTab').addClass('active');
  }
});

// Transition function
function getTransition() {
  return d3.transition().duration(600).ease(d3.easeSinInOut);
}

// A function that creates or updates the plot for a given variable
function update(val) {
  const updateTrans =
    // Parse the Data
    d3.json(`/api/stats/histogram_metric/` + val).then(function (data) {
      // X axis
      const xScale = d3
        .scaleBand()
        .range([0, width])
        .domain(data.map((d) => d.k))
        .padding(0.2);

        const xAxisGenerator = d3
            .axisBottom(xScale);

      const xAxis = svg
        .select('.x-axis')
        .transition(getTransition())
        .call(xAxisGenerator)
        .attr('transform', `translate(0, ${height})`)
        // `translate(0, ${height})`
        .selectAll('text')
        .attr('transform', 'translate(-10,0)rotate(-45)')
        .style('text-anchor', 'end');

      /*
            svg.append("g")

                .attr("transform", `translate(0, ${height})`)

                .call(d3.axisBottom(xScale))
                .transition(updateTrans)
                .selectAll("text")
                .attr("transform", "translate(-10,0)rotate(-45)")
                .style("text-anchor", "end");
    */
      // Y axis
      const yAccessor = function (d) {
        return d.v;
      };
      const yScale = d3
        .scaleLinear()
        .domain([0, d3.max(data, yAccessor)])
        .range([height, 0])
        .nice();

      const yAxisGenerator = d3.axisLeft(yScale);

      const yAxis = svg
        .select('.y-axis')
        .transition(getTransition())
        .call(yAxisGenerator);
      //.attr("transform", `translate(0, ${height})`)
      /*
                svg.append("g")
                .transition(updateTrans)
                .call(d3.axisLeft(yScale));
    */
      var u = svg.selectAll('rect').data(data);
      u.exit().remove();

      u.enter()
        .append('rect')
        .merge(u)
        .transition(getTransition())

        .attr('x', function (d) {
          return xScale(d.k);
        })
        .attr('y', function (d) {
          return yScale(d.v);
        })
        // Bar width
        .attr('width', xScale.bandwidth())
        // Bar height
        .attr('height', (d) => height - yScale(d.v))
        .attr('fill', 'steelblue');
    });
}
