package com.batch.lab.demo.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Employee {
    private String employeeName;
    private Long employeeId;
    private String employeeRole;
    private Long contact;
    private String location;
}
