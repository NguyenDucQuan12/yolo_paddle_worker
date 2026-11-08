"""
Chính sách restart cho từng tiến trình con: exponential backoff + circuit breaker theo cửa sổ trượt.

Ví dụ với max_restarts=5, window=900s, backoff 2s→120s, cooldown=600s:
    lỗi 1 → chờ 2s, lỗi 2 → 4s, lỗi 3 → 8s, ... (tối đa 120s)
    lỗi thứ 6 trong 15 phút → mở circuit: tạm ngưng 10 phút rồi thử lại từ đầu.
Worker chạy ổn định lâu hơn cửa sổ → các lỗi cũ trượt ra ngoài, bộ đếm tự về 0.
"""
import time
from collections import deque


class RestartPolicy:
    """
    Class kiểm soát số lần restart của một tiến trình con, theo cửa sổ trượt (sliding window) và cơ chế circuit breaker.
    - max_restarts: số lần restart tối đa trong cửa sổ (window_seconds).
    - window_seconds: thời gian cửa sổ trượt (tính bằng giây).
    - backoff_initial: thời gian chờ restart lần đầu (giây).
    - backoff_max: thời gian chờ restart tối đa (giây).
    - cooldown_seconds: thời gian mở circuit breaker khi vượt quá max_restarts (giây).
    - clock: hàm trả thời gian hiện tại (mặc định dùng time.monotonic).
    """
    def __init__(self, max_restarts, window_seconds, backoff_initial, backoff_max, cooldown_seconds,
                 clock=time.monotonic):
        self.max_restarts = int(max_restarts)
        self.window = float(window_seconds)
        self.backoff_initial = float(backoff_initial)
        self.backoff_max = float(backoff_max)
        self.cooldown = float(cooldown_seconds)
        self.clock = clock
        self._failures = deque()
        self.circuit_open_until = 0.0
        self.circuit_trips = 0

    @classmethod
    def from_config(cls, cfg, clock=time.monotonic):
        """
        Tạo RestartPolicy từ config.yaml.
        """
        sup = cfg['supervisor']
        return cls(cfg['queue']['max_restarts'], sup['restart_window_seconds'],
                   sup['restart_backoff_initial_seconds'], sup['restart_backoff_max_seconds'],
                   sup['circuit_cooldown_seconds'], clock=clock)

    def _prune(self, now):
        """
        Dọn dẹp các lỗi cũ ra khỏi hàng đợi.
        """
        # Nếu lỗi cũ đã vượt quá cửa sổ trượt, loại bỏ chúng khỏi hàng đợi.
        while self._failures and now - self._failures[0] > self.window:
            self._failures.popleft()

    @property
    def recent_failures(self):
        """
        :return: số lần process lỗi trong cửa sổ trượt.
        """
        self._prune(self.clock())
        return len(self._failures)

    def record_failure(self):
        """
        Ghi nhận một lần process lỗi.
        :return: (delay_seconds, circuit_opened)
        """
        # Lấy thời gian hiện tại
        now = self.clock()
        self._prune(now)

        # Thêm thời điểm lỗi hiện tại vào hàng đợi
        self._failures.append(now)
        # Tính tổng số lần lỗi trong hàng đợi
        count = len(self._failures)

        # Nếu số lần lỗi vượt quá max_restarts, mở circuit breaker và trả về thời gian cooldown.
        if count > self.max_restarts:
            self._failures.clear()
            self.circuit_open_until = now + self.cooldown
            self.circuit_trips += 1
            return self.cooldown, True

        # Nếu số lần lỗi chưa vượt quá max_restarts, tính thời gian chờ restart theo exponential backoff.
        # Công thức: delay = min(backoff_initial * 2^(count-1), backoff_max) tức là lần đầu 2s, lần 2 4s, lần 3 8s, ... nhưng không vượt quá backoff_max.
        return min(self.backoff_initial * 2 ** (count - 1), self.backoff_max), False

    def circuit_open(self):
        """
        Mở circuit breaker khi số lần lỗi vượt quá max_restarts trong cửa sổ trượt.
        :return: True nếu circuit đang mở (không nên restart), False nếu circuit đóng (có thể restart).
        """
        return self.clock() < self.circuit_open_until
