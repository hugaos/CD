import time
from collections import deque
import itertools

class Sudoku:
    def __init__(self, sudoku):
        self.grid = sudoku
        self.recent_requests = deque()
        self.initial_grid = [row[:] for row in sudoku]

    def __str__(self):
        string_representation = "| - - - - - - - - - - - |\n"

        for i in range(9):
            string_representation += "| "
            for j in range(9):
                string_representation += str(self.grid[i][j])
                string_representation += " | " if j % 3 == 2 else " "

            if i % 3 == 2:
                string_representation += "\n| - - - - - - - - - - - |"
            string_representation += "\n"

        return string_representation

    def check_row(self, row, base_delay=0.01, interval=10, threshold=5):
        self._limit_calls(base_delay, interval, threshold)
        if sum(self.grid[row]) != 45 or len(set(self.grid[row])) != 9:
            return False
        return True

    def check_column(self, col, base_delay=0.01, interval=10, threshold=5):
        self._limit_calls(base_delay, interval, threshold)
        if sum([self.grid[row][col] for row in range(9)]) != 45 or len(set([self.grid[row][col] for row in range(9)])) != 9:
            return False
        return True

    def check_square(self, row, col, base_delay=0.01, interval=10, threshold=5):
        self._limit_calls(base_delay, interval, threshold)
        square = [self.grid[row + i][col + j] for i in range(3) for j in range(3)]
        if sum(square) != 45 or len(set(square)) != 9:
            return False
        return True

    def check(self, base_delay=0.01, interval=10, threshold=5):
        for row in range(9):
            if not self.check_row(row, base_delay, interval, threshold):
                return False
        for col in range(9):
            if not self.check_column(col, base_delay, interval, threshold):
                return False
        for i in range(3):
            for j in range(3):
                if not self.check_square(i * 3, j * 3, base_delay, interval, threshold):
                    return False
        return True

    def _limit_calls(self, base_delay, interval, threshold):
        current_time = time.time()
        self.recent_requests.append(current_time)
        num_requests = len([t for t in self.recent_requests if current_time - t < interval])

        if num_requests > threshold:
            delay = base_delay * (num_requests - threshold + 1)
            time.sleep(delay)

    def is_valid_partial(self, part):
        for row in part:
            numbers = [num for num in row if num != 0]
            if len(numbers) != len(set(numbers)):
                return False

        for col in range(9):
            column = [row[col] for row in part if row[col] != 0]
            if len(column) != len(set(column)):
                return False

        return True

    def brute_force_solve(self, part, base_delay=0.01, interval=10, threshold=5):
        empty_positions = [(r, c) for r in range(len(part)) for c in range(9) if part[r][c] == 0]
        all_combinations = itertools.product(range(1, 10), repeat=len(empty_positions))
        validations = 0
        solutions = []
        for combination in all_combinations:
            for (row, col), num in zip(empty_positions, combination):
                part[row][col] = num
                validations += 1
            if self.is_valid_partial(part):
                self._limit_calls(base_delay, interval, threshold)  # Aplicar atras
                print("Trying combination:", combination)
                solutions.append([row[:] for row in part])
                time.sleep(base_delay);
            # Reset part to the original state for the next combination
            for (row, col), _ in zip(empty_positions, combination):
                part[row][col] = 0

        return solutions, validations
    
if __name__ == "__main__":
    sudoku = Sudoku([
        [8, 2, 7, 1, 5, 4, 3, 9, 6],
        [9, 6, 5, 3, 2, 7, 1, 4, 8], 
        [3, 4, 1, 6, 8, 9, 7, 5, 2],
        [5, 0, 3, 4, 6, 8, 2, 7, 1], 
        [4, 7, 2, 5, 1, 3, 6, 8, 9], 
        [6, 1, 8, 9, 7, 2, 4, 3, 5], 
        [7, 0, 6, 2, 3, 5, 9, 1, 4], 
        [1, 5, 4, 7, 9, 6, 8, 2, 3], 
        [2, 3, 9, 8, 4, 1, 5, 6, 7]
    ])

    print("Initial Sudoku:")
    print(sudoku)
