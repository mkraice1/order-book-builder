# Deephaven Book Builder example

This project provides an example of how to create a Top N of Book table from a live stream of quotes data.

To run the project you will need to have docker installed and then run
```bash
./gradlew :jar
cd docker && docker-compose up --build
```