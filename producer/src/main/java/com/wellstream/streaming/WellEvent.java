package com.wellstream.streaming;

public class WellEvent {
        public String well_id;
        public String facility_id;
        public long timestamp;
        public double methane_ppm;
        public double co2_ppm;
        public double pressure_psi;
        public double temperature_f;
        public double flow_rate_bpd;
        public String vibration_encoded;  // base64
        public String pump_status;
        public boolean maintenance_alert;
}
