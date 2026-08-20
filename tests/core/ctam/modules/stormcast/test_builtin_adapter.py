from EdgeWARN.ctam.builtins.stormcast import BuiltinStormCastAdapter


class _Host:
    def __init__(self):
        self.history_calls = []
        self.alert_calls = []

    def history(self, cell_id):
        self.history_calls.append(cell_id)
        return []

    def previous_alert(self, cell_id):
        self.alert_calls.append(cell_id)
        return None

    def publish(self, alerts):
        return len(alerts)


def test_builtin_adapter_reads_history_and_alert_state_through_host_service():
    host = _Host()
    adapter = BuiltinStormCastAdapter(host)
    cell = {
        "id": "cell-1",
        "timestamp": "2026-08-05T12:00:00+00:00",
        "centroid": [35.25, 262.75],
        "dx": 500.0,
        "dy": 250.0,
        "dt": 300.0,
        "properties": {
            "p100EchoTop30": 10.0,
            "EchoTop50": 8.0,
            "wind_field": {
                "u850": 12.0, "v850": 4.0,
                "u700": 14.0, "v700": 5.0,
                "u500": 18.0, "v500": 7.0,
                "u250": 22.0, "v250": 9.0,
            },
        },
        "modules": {},
    }

    adapter.run(cell)
    adapter.alerts(cell)

    assert host.history_calls == ["cell-1"]
    assert host.alert_calls == ["cell-1"]
    assert cell["modules"]["StormCast"]["status"] == "success"
