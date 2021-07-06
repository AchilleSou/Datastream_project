# Datastream_project
Final project for the datastream course of IASD Paris Dauphine-PSL.

## Requirements
 * Java 1.8
 * Scala
 * Docker

## How to download
In your terminal run  `git clone https://github.com/AchilleSou/Datastream_project.git`

## How to run 
In the main folder, build the docker container that runs kafka with `docker-compose up`.  
Open the `quickstart` folder as a new project in your favorite code editor then build and run it.

The suspect behaviors catched by the algorithm will be sent back to Kafka as a topic called "suspects". The flink job will display them in the terminal nevertheless.
