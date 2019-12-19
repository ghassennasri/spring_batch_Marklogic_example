package com.marklogic.mock.config;


import com.marklogic.mock.model.Employee;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class EmployeeRowMapper implements RowMapper<Employee> {

    @Override
    public Employee mapRow(ResultSet rs, int rowNum) throws SQLException {
        Employee person = new Employee();
        person.setEmpNo(rs.getInt("emp_no"));
        person.setFirstName(rs.getString("first_name"));
        person.setLastName(rs.getString("last_name"));
        person.setHireDate(rs.getDate("hire_date"));
        person.setBirthDate(rs.getDate("birth_date"));
        return person;
    }

}