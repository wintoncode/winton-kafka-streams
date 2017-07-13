# Roadmap

The roadmap is a high level overview of work we would like to see implemented  For more details and discussion of new features, improvements or bugs, please see the [issue list](https://github.com/wintoncode/winton-kafka-streams/issues) in GitHub. 

* Complete implementation of Kafka's Streams API in Python
    * The current code is a good proof of concept but is still under active development. There are a number of key features remaining, in particular a persistent state store and a DSL. There are also many improvements to existing features left to implement - check the issue list for the latest status. 
* Implement new features of Kafka's Streams API
    * v0.11 of Apache Kafka was released on 28 June 2017 with many important and useful features. 
* Investigate a more Pythonic API/DSL
    * The current Processor API follows the Java layout very closely. A Python Streams domain specific language (DSL) should leverage Python's unique language stregnths to make writing stream application as easy and intuitive as possible. 
* Optimise performance
    * Python has many known performance limitations; continue to optimise the code to perform as well as possible. Consider implementing some or all of the application in C. 
