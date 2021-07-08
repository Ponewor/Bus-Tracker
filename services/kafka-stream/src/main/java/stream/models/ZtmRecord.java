package stream.models;

import com.google.gson.annotations.SerializedName;

import java.util.Date;

public class ZtmRecord {
    @SerializedName("Lines")
    public String lines;
    @SerializedName("Lon")
    public double lon;
    @SerializedName("Lat")
    public double lat;
    @SerializedName("VehicleNumber")
    public String vehicleNumber;
    @SerializedName("Brigade")
    public String brigade;
    @SerializedName("Time")
    public Date time;
}
