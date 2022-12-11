package com.springboot.reactive.assignment2.entity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class EmployeeSerDes implements Serializer<EmployeeRequest>, Deserializer<EmployeeRequest> {
    public static final ObjectMapper mapper = JsonMapper.builder()
            .findAndAddModules()
            .build();
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, EmployeeRequest employeeRequest) {


//        byte[] employeeName = data.getEmpName().getBytes(StandardCharsets.UTF_8);
//        byte[] employeeCity = data.getEmpCity().getBytes(StandardCharsets.UTF_8);
//        byte[] employeePhone = data.getEmpPhone().getBytes(StandardCharsets.UTF_8);
////        byte[] javaExperience = data.getJavaExperience().t
////        byte[] springExperience
//        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + employeeName.length + 4 + employeeCity.length + 4 + employeePhone.length
//                +8+8);
//        buffer.putInt(data.getEmpId());
//        buffer.putInt(employeeName.length);
//        buffer.put(employeeName);
//        buffer.putInt(employeeCity.length);
//        buffer.put(employeeCity);
//        buffer.putInt(employeePhone.length);
//        buffer.put(employeePhone);
//        buffer.putDouble(data.getJavaExp());
//        buffer.putDouble(data.getSpringExp());
//        return buffer.array();
        try {
            return mapper.writeValueAsBytes(employeeRequest);
        } catch (JsonProcessingException e) {
            throw new SerializationException(e);
        }

    }

    @Override
    public EmployeeRequest deserialize(String topic, byte[] data) {
//        ByteBuffer buffer = ByteBuffer.wrap(data);
//        int employeeId = buffer.getInt();
//        byte[] name = new byte[buffer.getInt()];
//        buffer.get(name);
//        String employeeName = new String(name, StandardCharsets.UTF_8);
//        byte[] city = new byte[buffer.getInt()];
//        buffer.get(city);
//        String employeeCity = new String(city, StandardCharsets.UTF_8);
//        byte[] phone = new byte[buffer.getInt()];
//        buffer.get(phone);
//        String employeePhone = new String(phone, StandardCharsets.UTF_8);
//        double javaExperience = buffer.getDouble();
//        double springExperience = buffer.getDouble();
//        EmployeeRequest employeeRequest = new EmployeeRequest(employeeId,employeeName,employeeCity,employeePhone,
//                javaExperience,springExperience);
//        return employeeRequest;
        try {
            return mapper.readValue(data, EmployeeRequest.class);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }


    @Override
    public void close() {
        Serializer.super.close();
    }
}
