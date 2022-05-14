DROP TABLE IF EXISTS assign1_intern_tab;


create table if not exists assign1_intern_tab
(emp_id int , emp_name string, age int, job_title string, dept_id int, city string, salary int, kilos_from_home int) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

load data local inpath 'employee.csv' into table  assign1_intern_tab;