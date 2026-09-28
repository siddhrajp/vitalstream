package com.vitalstream.api.reading;

/** What a single reading measures. The unit is implied by the metric. */
public enum Metric {
    HEART_RATE,     // beats per minute
    SPO2,           // blood oxygen saturation, percent
    BP_SYSTOLIC,    // mmHg
    BP_DIASTOLIC,   // mmHg
    TEMPERATURE,    // degrees Celsius
    GLUCOSE         // mg/dL
}
