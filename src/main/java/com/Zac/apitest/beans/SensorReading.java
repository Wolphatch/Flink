package com.Zac.apitest.beans;



//传感器温度读书的数据类型
public class SensorReading {
    // 属性：id，timestamp，temperature
    private String id;
    private Long timestamp;
    private Double temperature;

    //Constructor
    public SensorReading(){

    }

    //Constructor with parameters
    public SensorReading(String id,Long timestamp,Double temperature){
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    //Getter and Setter

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }


    //toString
    @Override
    public String toString() {
        return "SensorReading{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", temperature=" + temperature +
                '}';
    }


}
