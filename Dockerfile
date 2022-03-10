FROM amirshayan/sbt:bullseye

WORKDIR /app

COPY . .

CMD sbt run
