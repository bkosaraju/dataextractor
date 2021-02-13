select
emp_no,
birth_date,
first_name,
last_name,
gender,
hire_date
from sparkTests.employees where hire_date='{{ hire_date }}';