# MetricHammer
A carbon line protocol test suite that can be used to simulate load on graphite servers.  This tool
will generate random metrics that you can define.  Although, I wrote this with the best possible performance
in mind, it still is written in Python and suffers some of the challenges related to the Python environment.  
But it does provide a good baseline.  You can always distribute multiple benchmarks accross nodes.  

At the end of each run, metrics files will be produced with times that you can use for further analysis.

## Requirements
Requires Python 3+, but works under python 2.7 just fine.

## Usage
./metrichammer.py -c configurationfile

NOTE: An example configuration file is provided with inline comments.

## Support
please submit an issue if you have any comments or concerns.

