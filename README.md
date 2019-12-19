# spring_batch_Marklogic_example

An example of a spring boot application using spring batch and Marklogic spring batch project.
Tha application connect to the famous Mysql sample database Employees, reads employees table
and serialize each tuple as an xml document into Marklogic database.

### prequisites:
* Both Mysql and Marklogic need to be running on localhost
  The docker-compose file used to test the project is included into this repository
  
* Mysql employees sample database need to be imported ( guidelines
on https://dev.mysql.com/doc/employee/en/employees-installation.html)

* Set up a Marklogic HTTP server on port 8011

