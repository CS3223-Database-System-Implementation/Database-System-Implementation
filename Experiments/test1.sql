SELECT Employees.eid, Employees.ename, Certified.eid, Certified.aid, Employees.salary
FROM Employees,Certified
WHERE Employees.eid=Certified.eid