SELECT Schedule.flno, Schedule.aid, Aircrafts.name
FROM Schedule,Aircrafts
WHERE Schedule.aid=Aircrafts.aid