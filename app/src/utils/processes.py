"""Helper dừng tiến trình: chỉ recover queue SAU KHI wait xác nhận đã chết."""
import subprocess  # Kiểm tra timeout của process.wait.


def stop_process(process, seconds):
    """
    Dừng tiến trình, chờ xác nhận đã chết, nếu timeout thì kill.
    :param process: đối tượng subprocess.Popen.
    :param seconds: thời gian chờ xác nhận đã chết, nếu timeout thì kill.
    """
    # process.poll() trả về None nếu tiến trình còn sống, hoặc returncode nếu đã chết.
    if process.poll() is None:
        # Cưỡng chế terminate
        process.terminate()
        try:
            # Chờ xác nhận đã chết, nếu timeout thì kill.
            process.wait(timeout=seconds)
        except subprocess.TimeoutExpired:
            # Ép buộc kill nếu terminate không thành công.
            process.kill()
            process.wait()

    # Trả về returncode của tiến trình, có thể là None nếu chưa chết, hoặc số nguyên nếu đã chết.
    return process.returncode
