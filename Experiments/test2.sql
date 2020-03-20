SELECT Flights.flno, Flights.from, Flights.to, Flights.distance, Schedule.aid
FROM Flights,Schedule
WHERE Flights.flno=Schedule.flno