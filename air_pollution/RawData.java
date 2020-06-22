package air_pollution_app.air_pollution;

public class RawData {

    String sensorId;     // The unique ID of the sensor.
    String position;     // "Latidide longitute" GPS coordinates
    String stamp;        // Timestamp when the measurement was taken
    int year;           // (optional) year where the measurement was taken.
    String type;         // The type of the value taken
    String value;        // The actual value

    public RawData(String sensorId,
            String position,
            String stamp,
            int year,
            String type,
            String value){
        this.sensorId = sensorId;
        this.position = position;
        this.stamp = stamp;
        this.year = year;
        this.type = type;
        this.value = value;
    }

    public RawData(String sensorId,
                   String position,
                   String stamp,
                   String type,
                   String value){
        this.sensorId = sensorId;
        this.position = position;
        this.stamp = stamp;
        this.type = type;
        this.value = value;
    }

    public String toString(){
        String s = "";
        if(sensorId == null){
            return null;
        }
        else{
            s = s+sensorId+",";
        }
        if(position == null){
            return null;
        }
        else{
            s = s+position+",";
        }
        if(stamp == null){
            return null;
        }
        else{
            s = s+stamp+",";
        }
        if(type == null){
            return null;
        }
        else{
            s = s+type+",";
        }
        if(value == null){
            return null;
        }
        else{
            s = s+value;
        }
        return s;
    }

    public RawData(){}

    public String getSensorId() {
        return sensorId;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public String getPosition() {
        return position;
    }

    public void setPosition(String position) {
        this.position = position;
    }

    public String getStamp() {
        return stamp;
    }

    public void setStamp(String stamp) {
        this.stamp = stamp;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}

