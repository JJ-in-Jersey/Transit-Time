from FileTools import FileTools as ft

class EastRiverValidation:

    current_sequence = ('flood_slack', 'slack_flood', 'slack_ebb', 'ebb_slack')

    def __init__(self, waypoints):
        hell_gate = list(filter(lambda wp: not bool(wp.unique_name.find('Hell_Gate')), waypoints))[0]
        if ft.file_exists(hell_gate.interpolation_data_file):
            self.hell_gate_df = ft.read_df(hell_gate.interpolation_data_file)
            first_current = self.hell_gate_df.at[0, ' Event']
            second_current = self.hell_gate_df.at[1, ' Event']
            print(first_current, second_current)
            pass
