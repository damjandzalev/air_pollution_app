package air_pollution;

public class Point implements Comparable<Object>{
	public String longitude;
	public String latitude;
	Point(String lon, String lat){
		if(lon == null || lat == null)
			throw new RuntimeException("Longitude or latitude can't be null!");
		longitude = lon;
		latitude = lat;
	}
	
	@Override
	public boolean equals(Object ob) {
		Point k = (Point) ob;
		if(k.longitude.equals(this.longitude) && k.latitude.equals(this.latitude)) {
			return true;
		}
		return false;
	}

	@Override
	public int compareTo(Object arg0) {
		Point point = (Point)(arg0);
		if(this.longitude.compareTo(point.longitude) != 0)
			return this.longitude.compareTo(point.longitude);
		return this.latitude.compareTo(point.latitude);
	}
}