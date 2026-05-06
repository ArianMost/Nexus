from abc import ABC, abstractmethod
from typing import Any, List, Union, Dict


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._results_list: List[str] = []
        self._counter: int = 0

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def output(self) -> tuple[int, str]:
        if not self._results_list:
            raise Exception("No data to print")

        value = self._results_list.pop(0)
        value_number = self._counter
        self._counter += 1
        return value_number, value


class NumericProcessor(DataProcessor):
    def ingest(self, data: Union[int, float, List[Union[int, float]]]) -> None:
        if not self.validate(data):
            raise Exception("Improper numeric data")

        if isinstance(data, (int, float)):
            self._results_list.append(str(data))
        else:
            for number in data:
                self._results_list.append(str(number))

    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            for number in data:
                if not isinstance(number, (int, float)):
                    return False
            return True
        return False


class TextProcessor(DataProcessor):
    def ingest(self, data: Union[str, List[str]]) -> None:
        if not self.validate(data):
            raise Exception("Improper text data")

        if isinstance(data, str):
            self._results_list.append(data)
        else:
            self._results_list.extend(data)

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            for txt in data:
                if not isinstance(txt, str):
                    return False
            return True
        return False


class LogProcessor(DataProcessor):
    def ingest(
            self, data: Union[Dict[str, str], List[Dict[str, str]]]
    ) -> None:
        if not self.validate(data):
            raise Exception("Improper log data")

        def format_log(data: Dict[str, str]) -> str:
            logs = []
            for value in data.values():
                logs.append(f"{value}")
            formatted_logs = ": ".join(logs)
            return formatted_logs

        if isinstance(data, dict):
            self._results_list.append(format_log(data))
        else:
            for entry in data:
                self._results_list.append(format_log(entry))

    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            for key, val in data.items():
                if not isinstance(key, str) or not isinstance(val, str):
                    return False
            return True

        if isinstance(data, list):
            for single_data in data:
                if not isinstance(single_data, dict):
                    return False
                for key, val in single_data.items():
                    if not isinstance(key, str) or not isinstance(val, str):
                        return False
            return True
        return False


def stream_processor() -> None:
    print("=== Code Nexus - Data Processor ===")
    print()

    print("Testing Numeric Processor...")
    num_proc = NumericProcessor()
    validation_stat = num_proc.validate(42)
    print(f"Trying to validate input '42': {validation_stat}")
    validation_stat = num_proc.validate('Hello')
    print(f"Trying to validate input 'Hello': {validation_stat}")
    print("Test invalid ingestion of string 'foo' without prior validation:")
    try:
        num_proc.ingest("foo")
    except Exception as e:
        print(f"Got exception: {e}")
    print("Processing data: [1, 2, 3, 4, 5]")
    num_proc.ingest([1, 2, 3, 4, 5])
    print("Extracting 3 values...")
    for _ in range(3):
        value_number, value = num_proc.output()
        print(f"Numeric value {value_number}: {value}")
    print()

    print("Testing Text Processor...")
    text_proc = TextProcessor()
    validation_stat = text_proc.validate(42)
    print(f"Trying to validate input '42': {validation_stat}")
    print("Processing data: ['Hello', 'Nexus', 'World']")
    text_proc.ingest(["Hello", "Nexus", "World"])
    print("Extracting 1 value...")
    value_number, value = text_proc.output()
    print(f"Text value {value_number}: {value}")
    print()

    print("Testing Log Processor...")
    log_proc = LogProcessor()
    validation_stat = log_proc.validate('Hello')
    print(f"Trying to validate input 'Hello': {validation_stat}")
    logs = [
        {"log_level": "NOTICE", "log_message": "Connection to server"},
        {"log_level": "ERROR", "log_message": "Unauthorized access!!"},
    ]
    print(f"Processing data: {logs}")
    log_proc.ingest(logs)
    print("Extracting 2 values...")
    for _ in range(2):
        value_number, value = log_proc.output()
        print(f"Log entry {value_number}: {value}")


if __name__ == "__main__":
    stream_processor()
