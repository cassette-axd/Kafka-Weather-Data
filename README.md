# School Project: Weather Data Collector App

## Overview
In this project we are given data about the weather in a specific location. The application the populates this data into a Kafka stream using a producer Python program. A consumer Python program consumes data from the stream to produce JSON files 
with summary stats, for use on a web dashboard. For simiplicity, the project uses a sing Kafka broker rather than a cluster. The data collected is then displayed on a graph using matplotlib.pyplot. The purpose of this project is to learn how to use
Kafka streaming to produce and collect data and then display that data visually.

## Functionality
Apache Kafka: an open-source distributed event streaming platform used by thousands of companies for high-performance data pipelines, streaming analytics, data integration, and mission-critical applications.

Matplotlib: a plotting library for the Python programming language and its numerical mathematics extension NumPy. It provides an object-oriented API for embedding plots into applications using general-purpose GUI toolkits like Tkinter, wxPython, 
Qt, or GTK.
