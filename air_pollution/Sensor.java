package air_pollution_app.air_pollution;

//https://skopje.pulse.eco/restapi example
class Sensor {
    private String sensorId;
    private String position;
    private String comments;
    private String type;
    private String description;
    private String status;

    public Sensor(String sensorId,
                      String position,
                      String comments,
                      String type,
                      String description,
                      String status) {
        this.sensorId = sensorId;
        this.position = position;
        this.comments = comments;
        this.type = type;
        this.description = description;
        this.status = status;
    }

    public Sensor() {

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
        if(comments == null){
            return null;
        }
        else{
            s = s+comments+",";
        }
        if(type == null){
            return null;
        }
        else{
            s = s+type+",";
        }
        if(description == null){
            return null;
        }
        else{
            s = s+description+",";
        }
        if(status == null){
            return null;
        }
        else{
            s = s+status;
        }
        return s;
    }

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

    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}