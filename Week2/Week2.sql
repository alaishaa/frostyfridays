create or replace schema frostyfriday;
use schema frostyfriday;

create stage ff_parquet_stage
    url = 's3://frostyfridaychallenges/challenge_2/';

list @ff_parquet_stage;
    
create file format parquet_file
type = parquet;

-- check file
-- select t.metadata$filename, t.metadata$file_row_number, t.$1 from @ff_parquet_stage (file_format => 'parquet_file', pattern=>'challenge_2/employees.parquet') as t;

create or replace temporary table temp_employees (
    city varchar,
    country varchar,
    country_code varchar,
    dept varchar,
    education varchar,
    email varchar,
    employee_id number,
    first_name varchar,
    job_title varchar,
    last_name varchar,
    payroll_iban varchar,
    postcode varchar,
    street_name varchar,
    street_num number,
    time_zone varchar,
    title varchar
  );

copy into temp_employees from (select 
    $1:city,
    $1:country,
    $1:country_code,
    $1:dept,
    $1:education,
    $1:email,
    $1:employee_id number,
    $1:first_name,
    $1:job_title,
    $1:last_name,
    $1:payroll_iban,
    $1:postcode,
    $1:street_name,
    $1:street_num number,
    $1:time_zone,
    $1:title
from @ff_parquet_stage (file_format => 'parquet_file', pattern=>'challenge_2/employees.parquet'));

drop file format parquet_file;
drop stage ff_parquet_stage;

create or replace view frostyfriday.employees as select dept, job_title from temp_employees;

create or replace stream employees_view_stream on view employees;

-- given commands from site
update temp_employees set country = 'japan' where employee_id = 8;
update temp_employees set last_name = 'forester' where employee_id = 22;
update temp_employees set dept = 'marketing' where employee_id = 25;
update temp_employees set title = 'ms' where employee_id = 32;
update temp_employees set job_title = 'senior financial analyst' where employee_id = 68;


select * from employees_view_stream;
