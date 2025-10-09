

# Schedules EdgeWARN process if theres new data for all datasets, otherwise waits another 10 sec before checking

class SchedulerUtils:
    def __init__(self, base_dir):
        self.base_dir = base_dir