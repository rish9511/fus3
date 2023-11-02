import psutil
import subprocess
import time

def get_network_speed(interface, duration=1):
    '''
    :param interface:
    :param duration:
    :return: sent_speed, recv_speed

    Check bytes sent & bytes received after every second to get the
    network speed
    '''

    # Initialize the counters
    initial_bytes_sent = psutil.net_io_counters(pernic=True)[interface].bytes_sent
    initial_bytes_recv = psutil.net_io_counters(pernic=True)[interface].bytes_recv

    # Sleep for the specified duration
    time.sleep(duration)

    # Get the updated counters
    final_bytes_sent = psutil.net_io_counters(pernic=True)[interface].bytes_sent
    final_bytes_recv = psutil.net_io_counters(pernic=True)[interface].bytes_recv

    # Calculate the speed in bytes per second
    sent_speed = (final_bytes_sent - initial_bytes_sent) / duration
    recv_speed = (final_bytes_recv - initial_bytes_recv) / duration

    # Convert to MB/s
    sent_speed = sent_speed / (1024 * 1024)
    recv_speed = recv_speed / (1024 * 1024)

    return sent_speed, recv_speed


def get_active_network_interface():
    try:
        # Get the network interface with the highest number of bytes sent
        interface, _ = max(psutil.net_io_counters(pernic=True).items(), key=lambda x: x[1].bytes_sent)
        return interface
    except Exception as e:
        return None


if __name__ == "__main__":
    active_interface = get_active_network_interface()
    if active_interface:
        print(f"Active network interface: {active_interface}")
        while True:
            sent_speed, recv_speed = get_network_speed(active_interface)
            ns_str = "Upload Speed: {:.2f} MB/s | Download Speed: {:.2f} MB/s"
            print(ns_str.format(sent_speed, recv_speed), end='\r')
    else:
        print("No active network interface found.")

