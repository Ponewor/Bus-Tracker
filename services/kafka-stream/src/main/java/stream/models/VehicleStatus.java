package stream.models;

import java.util.Date;

public class VehicleStatus {
    public String lines;
    public double lon;
    public double lat;
    public String vehicleNumber;
    public String brigade;
    public Date time;
    public double speed;
    public double bearing;

    public VehicleStatus(ZtmRecord record) {
        this.lines = record.lines;
        this.lon = record.lon;
        this.lat = record.lat;
        this.vehicleNumber = record.vehicleNumber;
        this.brigade = record.brigade;
        this.time = record.time;
    }
}
