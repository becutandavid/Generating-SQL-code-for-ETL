
 -- DDL for dim_instructor
CREATE TABLE dim_instructor(
  skey serial primary key,
  id int4 NOT NULL,
  firstname varchar NOT NULL,
  lastname varchar NOT NULL,
  sex bpchar NOT NULL,
  email varchar NOT NULL,
  age int4 NOT NULL,
  country varchar NOT NULL,
  userid int4,
  start_date timestamp NOT NULL default NOW(),
  end_date timestamp default '9999-12-31'
);

 -- Procedure for dim_instructor
CREATE OR REPLACE PROCEDURE sp_performETL_Instructor()
LANGUAGE PLPGSQL
AS $$
BEGIN

INSERT INTO dim_instructor(id,firstname,lastname,sex,email,age,country,userid)
SELECT platforma.instructors.id, platforma.users.firstname, platforma.users.lastname, platforma.users.sex, platforma.users.email, platforma.instructors.age, platforma.instructors.country, platforma.instructors.userid
FROM platforma.instructors INNER JOIN platforma.users on (platforma.instructors.userid = platforma.users.id) LEFT OUTER JOIN dim_instructor on (platforma.instructors.userid = dim_instructor.userid)
WHERE dim_instructor.skey is NULL;

-- find modified
CREATE TEMP TABLE scd2 AS
SELECT dim_instructor.skey, platforma.instructors.id, platforma.users.firstname, platforma.users.lastname, platforma.users.sex, platforma.users.email, platforma.instructors.age, platforma.instructors.country, platforma.instructors.userid
FROM platforma.instructors INNER JOIN platforma.users on (platforma.instructors.userid = platforma.users.id) INNER JOIN dim_instructor on (platforma.instructors.userid = dim_instructor.userid) and (platforma.instructors.id != dim_instructor.id or platforma.users.firstname != dim_instructor.firstname or platforma.users.lastname != dim_instructor.lastname or platforma.users.sex != dim_instructor.sex or platforma.users.email != dim_instructor.email or platforma.instructors.age != dim_instructor.age or platforma.instructors.country != dim_instructor.country)
WHERE dim_instructor.end_date = '9999-12-31';

-- update table
UPDATE dim_instructor
SET end_date=NOW()
FROM scd2
WHERE scd2.skey = dim_instructor.skey;

-- add updated rows
INSERT INTO dim_instructor(id, firstname, lastname, sex, email, age, country, userid)
SELECT id, firstname, lastname, sex, email, age, country, userid
FROM scd2;

DROP TABLE scd2;
END $$;


 -- DDL for dim_course
CREATE TABLE dim_course(
  skey serial primary key,
  id int4 NOT NULL,
  name varchar NOT NULL,
  description varchar,
  price int4 NOT NULL,
  id int4 NOT NULL,
  name varchar NOT NULL,
  description varchar,
  price int4 NOT NULL,
  courseid int4,
  start_date timestamp NOT NULL default NOW(),
  end_date timestamp default '9999-12-31'
);

 -- Procedure for dim_course
CREATE OR REPLACE PROCEDURE sp_performETL_Course()
LANGUAGE PLPGSQL
AS $$
BEGIN

INSERT INTO dim_course(id,name,description,price,id,name,description,price,courseid)
SELECT platforma.courses.id, platforma.courses.name, platforma.courses.description, platforma.prices.price, platforma.courses.id, platforma.courses.name, platforma.courses.description, platforma.prices.price, platforma.prices.courseid
FROM platforma.prices INNER JOIN platforma.courses on (platforma.prices.courseid = platforma.courses.id) LEFT OUTER JOIN dim_course on (platforma.prices.courseid = dim_course.courseid)
WHERE dim_course.skey is NULL;

-- find modified
CREATE TEMP TABLE scd2 AS
SELECT dim_course.skey, platforma.courses.id, platforma.courses.name, platforma.courses.description, platforma.prices.price, platforma.courses.id, platforma.courses.name, platforma.courses.description, platforma.prices.price, platforma.prices.courseid
FROM platforma.prices INNER JOIN platforma.courses on (platforma.prices.courseid = platforma.courses.id) INNER JOIN dim_course on (platforma.prices.courseid = dim_course.courseid) and (platforma.courses.id != dim_course.id or platforma.courses.name != dim_course.name or platforma.courses.description != dim_course.description or platforma.prices.price != dim_course.price or platforma.courses.id != dim_course.id or platforma.courses.name != dim_course.name or platforma.courses.description != dim_course.description or platforma.prices.price != dim_course.price)
WHERE dim_course.end_date = '9999-12-31';

-- update table
UPDATE dim_course
SET end_date=NOW()
FROM scd2
WHERE scd2.skey = dim_course.skey;

-- add updated rows
INSERT INTO dim_course(id, name, description, price, id, name, description, price, courseid)
SELECT id, name, description, price, id, name, description, price, courseid
FROM scd2;

DROP TABLE scd2;
END $$;


 -- DDL for dim_students
CREATE TABLE dim_students(
  skey serial primary key,
  userid int4,
  firstname varchar NOT NULL,
  lastname varchar NOT NULL,
  start_date timestamp NOT NULL default NOW(),
  end_date timestamp default '9999-12-31'
);

 -- Procedure for dim_students
CREATE OR REPLACE PROCEDURE sp_performETL_Students()
LANGUAGE PLPGSQL
AS $$
BEGIN

INSERT INTO dim_students(userid,firstname,lastname)
SELECT platforma.students.userid, platforma.users.firstname, platforma.users.lastname
FROM platforma.students INNER JOIN platforma.users on (platforma.students.userid = platforma.users.id) LEFT OUTER JOIN dim_students on (platforma.students.userid = dim_students.userid)
WHERE dim_students.skey is NULL;

-- find modified
CREATE TEMP TABLE scd2 AS
SELECT dim_students.skey, platforma.students.userid, platforma.users.firstname, platforma.users.lastname
FROM platforma.students INNER JOIN platforma.users on (platforma.students.userid = platforma.users.id) INNER JOIN dim_students on (platforma.students.userid = dim_students.userid) and (platforma.users.firstname != dim_students.firstname or platforma.users.lastname != dim_students.lastname)
WHERE dim_students.end_date = '9999-12-31';

-- update table
UPDATE dim_students
SET end_date=NOW()
FROM scd2
WHERE scd2.skey = dim_students.skey;

-- add updated rows
INSERT INTO dim_students(userid, firstname, lastname)
SELECT userid, firstname, lastname
FROM scd2;

DROP TABLE scd2;
END $$;


 -- DDL for dim_admins
CREATE TABLE dim_admins(
  skey serial primary key,
  userid int4,
  firstname varchar NOT NULL,
  lastname varchar NOT NULL,
  start_date timestamp NOT NULL default NOW(),
  end_date timestamp default '9999-12-31'
);

 -- Procedure for dim_admins
CREATE OR REPLACE PROCEDURE sp_performETL_Admins()
LANGUAGE PLPGSQL
AS $$
BEGIN

INSERT INTO dim_admins(userid,firstname,lastname)
SELECT platforma.admins.userid, platforma.users.firstname, platforma.users.lastname
FROM platforma.admins INNER JOIN platforma.users on (platforma.admins.userid = platforma.users.id) LEFT OUTER JOIN dim_admins on (platforma.admins.userid = dim_admins.userid)
WHERE dim_admins.skey is NULL;

-- find modified
CREATE TEMP TABLE scd2 AS
SELECT dim_admins.skey, platforma.admins.userid, platforma.users.firstname, platforma.users.lastname
FROM platforma.admins INNER JOIN platforma.users on (platforma.admins.userid = platforma.users.id) INNER JOIN dim_admins on (platforma.admins.userid = dim_admins.userid) and (platforma.users.firstname != dim_admins.firstname or platforma.users.lastname != dim_admins.lastname)
WHERE dim_admins.end_date = '9999-12-31';

-- update table
UPDATE dim_admins
SET end_date=NOW()
FROM scd2
WHERE scd2.skey = dim_admins.skey;

-- add updated rows
INSERT INTO dim_admins(userid, firstname, lastname)
SELECT userid, firstname, lastname
FROM scd2;

DROP TABLE scd2;
END $$;

CALL sp_performETL_Instructor();
CALL sp_performETL_Course();
CALL sp_performETL_Students();
CALL sp_performETL_Admins();
