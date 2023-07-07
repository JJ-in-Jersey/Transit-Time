from tt_file_tools import file_tools as ft

class EastRiverValidation:

    slack_sequence = {'flood_slack':'ebb_begins', 'slack_flood':'flood_begins', 'slack_ebb':'ebb_begins', 'ebb_slack':'flood_begins'}

    def __init__(self, waypoints):
        hell_gate = list(filter(lambda wp: not bool(wp.unique_name.find('Hell_Gate')), waypoints))[0]
        if ft.csv_npy_file_exists(hell_gate.interpolation_data_file):
            self.hell_gate_df = ft.read_df(hell_gate.interpolation_data_file)
            first_current = self.hell_gate_df.at[0, 'Event']
            second_current = self.hell_gate_df.at[1, 'Event']
            print(f'{first_current}_{second_current}')
            pass
