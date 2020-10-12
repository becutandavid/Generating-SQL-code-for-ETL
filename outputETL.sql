
 -- DDL for dim_admins
CREATE TABLE dim_admins(
  skey serial primary key,
  userid int4,
  firstname varchar NOT NULL,
  lastname varchar NOT NULL,
  start_date timestamp NOT NULL default(NOW()),
  end_date timestamp default("9999-12-31")
);

 -- Procedure for dim_admins
CREATE OR REPLACE PROCEDURE sp_performETL_Admins
LANGUAGE PLPGSQL
AS $$
BEGIN

INSERT INTO dim_admins
SELECT platforma.admins.userid, platforma.users.firstname, platforma.users.lastname
FROM platforma.admins INNER JOIN platforma.users on (platforma.admins.userid = platforma.users.id) LEFT OUTER JOIN dim_admins on (platforma.admins.userid = dim_admins.userid)
WHERE platforma.admins.skey is NULL

-- find modified
SELECT dim_admins.skey, platforma.admins.userid, platforma.users.firstname, platforma.users.lastname
INTO #scd2
FROM platforma.admins INNER JOIN platforma.users on (platforma.admins.userid = platforma.users.id) INNER JOIN dim_admins on (platforma.admins.userid = dim_admins.userid) and (platforma.users.firstname != dim_admins.firstname or platforma.users.lastname != dim_admins.lastname)
WHERE dim_admins.end_date = "9999-12-31"

-- update table
UPDATE dim_admins
SET end_date=NOW()
FROM #scd2
WHERE #scd2.skey = dim_admins.skey

-- add updated rows
INSERT INTO dim_admins(userid, firstname, lastname)
SELECT userid, firstname, lastname
FROM #scd2

END; $$


 -- DDL for dim_course
CREATE TABLE dim_course(
  skey serial primary key,
  id int4 NOT NULL,
  name varchar NOT NULL,
  description varchar,
  start_date timestamp NOT NULL default(NOW()),
  end_date timestamp default("9999-12-31")
);

 -- Procedure for dim_course
CREATE OR REPLACE PROCEDURE sp_performETL_Course
LANGUAGE PLPGSQL
AS $$
BEGIN

INSERT INTO dim_course
SELECT platforma.courses.id, platforma.courses.name, platforma.courses.description
FROM platforma.courses LEFT OUTER JOIN dim_course on (platforma.courses.id = dim_course.id)
WHERE platforma.courses.skey is NULL

-- find modified
SELECT dim_course.skey, platforma.courses.id, platforma.courses.name, platforma.courses.description
INTO #scd2
FROM platforma.courses INNER JOIN dim_course on (platforma.courses.id = dim_course.id) and (platforma.courses.name != dim_course.name or platforma.courses.description != dim_course.description)
WHERE dim_course.end_date = "9999-12-31"

-- update table
UPDATE dim_course
SET end_date=NOW()
FROM #scd2
WHERE #scd2.skey = dim_course.skey

-- add updated rows
INSERT INTO dim_course(id, name, description)
SELECT id, name, description
FROM #scd2

END; $$


 -- DDL for dim_students
CREATE TABLE dim_students(
  skey serial primary key,
  userid int4,
  firstname varchar NOT NULL,
  lastname varchar NOT NULL,
  start_date timestamp NOT NULL default(NOW()),
  end_date timestamp default("9999-12-31")
);

 -- Procedure for dim_students
CREATE OR REPLACE PROCEDURE sp_performETL_Students
LANGUAGE PLPGSQL
AS $$
BEGIN

INSERT INTO dim_students
SELECT platforma.students.userid, platforma.users.firstname, platforma.users.lastname
FROM platforma.students INNER JOIN platforma.users on (platforma.students.userid = platforma.users.id) LEFT OUTER JOIN dim_students on (platforma.students.userid = dim_students.userid)
WHERE platforma.students.skey is NULL

-- find modified
SELECT dim_students.skey, platforma.students.userid, platforma.users.firstname, platforma.users.lastname
INTO #scd2
FROM platforma.students INNER JOIN platforma.users on (platforma.students.userid = platforma.users.id) INNER JOIN dim_students on (platforma.students.userid = dim_students.userid) and (platforma.users.firstname != dim_students.firstname or platforma.users.lastname != dim_students.lastname)
WHERE dim_students.end_date = "9999-12-31"

-- update table
UPDATE dim_students
SET end_date=NOW()
FROM #scd2
WHERE #scd2.skey = dim_students.skey

-- add updated rows
INSERT INTO dim_students(userid, firstname, lastname)
SELECT userid, firstname, lastname
FROM #scd2

END; $$

CALL sp_performETL_Admins;
CALL sp_performETL_Course;
CALL sp_performETL_Students;
