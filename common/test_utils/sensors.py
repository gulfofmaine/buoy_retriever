import dagster as dg


def get_sensor_by_name(defs, name: str) -> dg.SensorDefinition:
    """Extract a specific sensor from Dagster Definitions by name.

    Uses the function name, not the full sensor key/prefix.
    """
    for sensor in defs.sensors:
        if sensor.name == name:
            return sensor
    raise KeyError(f"Sensor with name {name} not found")
