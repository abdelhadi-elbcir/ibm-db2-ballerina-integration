import ballerina/http;
import ballerina/sql;
import ballerina/log;
import ballerinax/java.jdbc as jdbc;

// Database configuration
configurable string dbHost = "localhost";
configurable int dbPort = 50000;
configurable string dbName = "testdb";
configurable string dbUser = "db2inst1";
configurable string dbPassword = "password123";

// Database client
final jdbc:Client dbClient = check new (
    url = string `jdbc:db2://${dbHost}:${dbPort}/${dbName}`,
    user = dbUser,
    password = dbPassword
);

// Employee record type
public type Employee record {|
    int id?;
    string first_name;
    string last_name;
    string email;
    string department?;
    decimal salary?;
    string hire_date?;
|};

// HTTP service
service /api/v1 on new http:Listener(8080) {
    
    // GET all employees
    resource function get employees() returns Employee[]|http:InternalServerError {
        do {
            sql:ParameterizedQuery query = `SELECT id, first_name, last_name, email, department, salary, hire_date FROM employees`;
            stream<Employee, sql:Error?> resultStream = dbClient->query(query);
            Employee[] employees = check from Employee employee in resultStream
                                   select employee;
            check resultStream.close();
            return employees;
        } on fail error e {
            log:printError("Error retrieving employees", e);
            return <http:InternalServerError>{
                body: {message: "Error retrieving employees"}
            };
        }
    }

    // GET employee by ID
    resource function get employees/[int id]() returns Employee|http:NotFound|http:InternalServerError {
        do {
            sql:ParameterizedQuery query = `SELECT id, first_name, last_name, email, department, salary, hire_date 
                                           FROM employees WHERE id = ${id}`;
            Employee|sql:Error result = dbClient->queryRow(query);
            
            if result is sql:NoRowsError {
                return <http:NotFound>{
                    body: {message: string `Employee with ID ${id} not found`}
                };
            }
            
            return check result;
        } on fail error e {
            log:printError(string `Error retrieving employee with ID ${id}`, e);
            return <http:InternalServerError>{
                body: {message: "Error retrieving employee"}
            };
        }
    }

    // POST - Create new employee
    resource function post employees(@http:Payload Employee employee) returns http:Created|http:BadRequest|http:InternalServerError {
        do {
            sql:ParameterizedQuery query = `INSERT INTO employees (first_name, last_name, email, department, salary, hire_date) 
                                           VALUES (${employee.first_name}, ${employee.last_name}, ${employee.email}, 
                                                  ${employee.department}, ${employee.salary}, ${employee.hire_date})`;
            sql:ExecutionResult result = check dbClient->execute(query);
            
            if result.affectedRowCount > 0 {
                return <http:Created>{
                    headers: {location: string `/api/v1/employees/${result.lastInsertId.toString()}`},
                    body: {message: "Employee created successfully", id: result.lastInsertId}
                };
            } else {
                return <http:BadRequest>{
                    body: {message: "Failed to create employee"}
                };
            }
        } on fail error e {
            log:printError("Error creating employee", e);
            return <http:InternalServerError>{
                body: {message: "Error creating employee"}
            };
        }
    }

    // PUT - Update employee by ID
    resource function put employees/[int id](@http:Payload Employee employee) returns http:Ok|http:NotFound|http:InternalServerError {
        do {
            // First check if employee exists
            sql:ParameterizedQuery checkQuery = `SELECT id FROM employees WHERE id = ${id}`;
            int|sql:Error existsResult = dbClient->queryRow(checkQuery);
            
            if existsResult is sql:NoRowsError {
                return <http:NotFound>{
                    body: {message: string `Employee with ID ${id} not found`}
                };
            }
            
            // Update the employee
            sql:ParameterizedQuery updateQuery = `UPDATE employees SET 
                                                 first_name = ${employee.first_name},
                                                 last_name = ${employee.last_name},
                                                 email = ${employee.email},
                                                 department = ${employee.department},
                                                 salary = ${employee.salary},
                                                 hire_date = ${employee.hire_date}
                                                 WHERE id = ${id}`;
            sql:ExecutionResult result = check dbClient->execute(updateQuery);
            
            return <http:Ok>{
                body: {message: "Employee updated successfully", affectedRows: result.affectedRowCount}
            };
        } on fail error e {
            log:printError(string `Error updating employee with ID ${id}`, e);
            return <http:InternalServerError>{
                body: {message: "Error updating employee"}
            };
        }
    }

    // DELETE employee by ID
    resource function delete employees/[int id]() returns http:Ok|http:NotFound|http:InternalServerError {
        do {
            sql:ParameterizedQuery query = `DELETE FROM employees WHERE id = ${id}`;
            sql:ExecutionResult result = check dbClient->execute(query);
            
            if result.affectedRowCount > 0 {
                return <http:Ok>{
                    body: {message: "Employee deleted successfully"}
                };
            } else {
                return <http:NotFound>{
                    body: {message: string `Employee with ID ${id} not found`}
                };
            }
        } on fail error e {
            log:printError(string `Error deleting employee with ID ${id}`, e);
            return <http:InternalServerError>{
                body: {message: "Error deleting employee"}
            };
        }
    }

    // GET employees by department
    resource function get employees/department/[string dept]() returns Employee[]|http:InternalServerError {
        do {
            sql:ParameterizedQuery query = `SELECT id, first_name, last_name, email, department, salary, hire_date 
                                           FROM employees WHERE UPPER(department) = UPPER(${dept})`;
            stream<Employee, sql:Error?> resultStream = dbClient->query(query);
            Employee[] employees = check from Employee employee in resultStream
                                   select employee;
            check resultStream.close();
            return employees;
        } on fail error e {
            log:printError(string `Error retrieving employees from department ${dept}`, e);
            return <http:InternalServerError>{
                body: {message: "Error retrieving employees"}
            };
        }
    }
}