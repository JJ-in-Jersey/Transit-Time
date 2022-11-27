



class TransitTimeJob:
    def __init__(self):
        pass
    def execute(self):
        pass
    def execute_callback(self):
        pass
    def error_callback(self):
        pass

# def ttSum(row, columns, timeStepInMinutes, inputDF):
#     tt = 0
#     rowIncrement = 0
#     for col in columns:
#         val = inputDF.loc[row + rowIncrement, col]
#         rowIncrement += int(inputDF.loc[row + rowIncrement, col]/timeStepInMinutes)
#         if val < 0:
#             print('transit time: negative elapsed time value')
#         else:
#             tt += val
#     return tt
#
