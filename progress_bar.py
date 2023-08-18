import time
import sys

total_units = 30
filled_units = 0

def calculate_progress(percent_uploaded: float) -> str:
    global filled_units
    uploaded_units = (total_units * (percent_uploaded/100))
    filled_units = uploaded_units
    return filled_units


def show_progress(percent_uploaded: float) -> str:

    total_uploaded = calculate_progress(percent_uploaded)
    sys.stdout.write("\r[{}{}] {}%".format("*" * int(total_uploaded), " " * (total_units - int(total_uploaded)), percent_uploaded))


# print('\n')
# show_progress(28.9)
# time.sleep(1)
# show_progress(35)
# time.sleep(1)
# show_progress(50)
# time.sleep(2)
# show_progress(60)
# time.sleep(2)
# show_progress(70)
# time.sleep(2)
# show_progress(80)
# time.sleep(2)
# show_progress(93)
# time.sleep(2)
# show_progress(98)
# time.sleep(2)
# show_progress(100)
# print('\n')
# 17. 82 %
#
# [===============================]
#
# 30 units long
#
# 17.82
