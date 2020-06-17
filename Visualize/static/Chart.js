       </center>
       <script>
          var wordChart = document.getElementById("chart");
          var twitterWordChart = new Chart(wordChart, {
               type: 'horizontalBar',
               data: {
                   labels: [{% for item in labels %}
                             "{{item}}",
                            {% endfor %}],
                   datasets: [{
                       label: 'number of count',
                       data: [{% for item in values %}
                                 {{item}},
                               {% endfor %}],
                       backgroundColor: [
                           'rgba(255, 99, 132, 0.2)',
                           'rgba(54, 162, 235, 0.2)',
                           'rgba(255, 206, 86, 0.2)',
                           'rgba(75, 192, 192, 0.2)',
                           'rgba(153, 102, 255, 0.2)',
                           'rgba(255, 159, 64, 0.2)',
                           'rgba(255, 99, 132, 0.2)',
                           'rgba(54, 162, 235, 0.2)',
                           'rgba(255, 206, 86, 0.2)'
                       ],
                       borderColor: [
                           'rgba(255,99,132,1)',
                           'rgba(54, 162, 235, 1)',
                           'rgba(255, 206, 86, 1)',
                           'rgba(75, 192, 192, 1)',
                           'rgba(153, 102, 255, 1)',
                           'rgba(255, 159, 64, 1)',
                           'rgba(255,99,132,1)',
                           'rgba(54, 162, 235, 1)',
                           'rgba(255, 206, 86, 1)'
                       ],
                       borderWidth: 1
                   }]
               },
               options: {
                   scales: {
                       yAxes: [{
                           ticks: {
                               beginAtZero:true
                           }
                       }]
                   }
               }
          });
          var sourceCounts = [];
          var sourceWords = [];
           setInterval(function(){
               $.getJSON('/updateChart', {
               }, function(data) {
                   sourceWords = data.sWords;
                   sourceCounts = data.sCounts;
               });
               twitterWordChart.data.datasets[0].data = sourceCounts;
               twitterWordChart.data.labels = sourceWords;
               twitterWordChart.update();
           },1500);
       </script>
   </body>
</html>
