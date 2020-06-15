package air_pollution;

public class SimRun{
	public String id;
	public String time;
	public String run_time;
	public SimRun(String time, String run_time) {
		this.run_time = run_time;
		this.time = time;
	}
	
	public void setId(String id) {
		this.id = id;
	}
	
	@Override
	public boolean equals(Object ob) {
		SimRun k = (SimRun) ob;
		if(k.time.equals(this.time) && k.run_time.equals(this.run_time)) {
			return true;
		}
		return false;
	}
}