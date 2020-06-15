package air_pollution;

public class PredictionPollutant implements Comparable<Object>{
	public String sim_r_id;
	public String sim_p_id;
	public PredictionPollutant(String sim_p_id, String sim_r_id) {
		this.sim_p_id = sim_p_id;
		this.sim_r_id = sim_r_id;
	}
	
	@Override
	public boolean equals(Object ob) {
		PredictionPollutant k = (PredictionPollutant) ob;
		if(k.sim_r_id.equals(this.sim_r_id) && k.sim_p_id.equals(this.sim_p_id)) {
			return true;
		}
		return false;
	}

	@Override
	public int compareTo(Object arg0) {
		PredictionPollutant pp = (PredictionPollutant)(arg0);
		if(this.sim_p_id.compareTo(pp.sim_p_id) == 0)
			return this.sim_r_id.compareTo(pp.sim_r_id);
		return this.sim_p_id.compareTo(pp.sim_p_id);
	}
}
