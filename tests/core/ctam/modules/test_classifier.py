from EdgeWARN.ctam.modules.Classifier import ClassifierModule


def test_classifier_module_run_is_noop():
    module = ClassifierModule()
    storm_entry = {"id": "cell_1", "properties": {}}

    module.run(storm_entry)

    assert "modules" not in storm_entry
