from abc import ABC, abstractmethod
from typing import Any, List, Union, Dict, Protocol


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._results_list: List[str] = []
        self._counter: int = 0
        self._total_processed: int = 0

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

    def total_processed(self) -> int:
        return self._total_processed

    def remaining(self) -> int:
        return len(self._results_list)


class NumericProcessor(DataProcessor):
    def ingest(self, data: Union[int, float, List[Union[int, float]]]) -> None:
        if not self.validate(data):
            raise Exception("Improper numeric data")

        if isinstance(data, (int, float)):
            self._results_list.append(str(data))
            self._total_processed += 1
        else:
            for number in data:
                self._results_list.append(str(number))
                self._total_processed += 1

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
            self._total_processed += 1
        else:
            self._results_list.extend(data)
            self._total_processed += len(data)

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
            self._total_processed += 1
        else:
            for entry in data:
                self._results_list.append(format_log(entry))
                self._total_processed += 1

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


class ExportPlugin(Protocol):
    def process_output(self, data: List[tuple[int, str]]) -> None:
        ...


class JSONExportPlugin:
    def process_output(self, data: List[tuple[int, str]]) -> None:
        print("JSON Output:")
        items = []
        for index, value in data:
            items.append(f'"item_{index}": "{value}"')
        formatted_items = ", ".join(items)
        print(f'{{{formatted_items}}}')


class CSVExportPlugin:
    def process_output(self, data: List[tuple[int, str]]) -> None:
        print("CSV Output:")
        items = []
        for _, value in data:
            items.append(value)
        formatted_items = ",".join(items)
        print(formatted_items)


class DataStream:
    def __init__(self) -> None:
        self._processors: List[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: List[Any]) -> None:
        for data in stream:
            processed = False
            for processor in self._processors:
                if processor.validate(data):
                    processor.ingest(data)
                    processed = True
                    break

            if not processed:
                print(f"DataStream error - "
                      f"Can't process element in stream: {data}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self._processors:
            print("No processor found, no data")
            return

        for proc in self._processors:
            titel = proc.__class__.__name__.replace("Processor", " Processor")
            print(
                f"{titel}: total {proc.total_processed()} items processed, "
                f"remaining {proc.remaining()} on processor"
            )

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for processor in self._processors:
            processed_data_list: List[tuple[int, str]] = []

            for _ in range(nb):
                try:
                    processed_data_list.append(processor.output())
                except Exception:
                    break

            if processed_data_list:
                plugin.process_output(processed_data_list)


def data_pipeline() -> None:
    print("=== Code Nexus - Data Pipeline ===")
    print()
    print("Initialize Data Stream...")
    print()

    data_stream = DataStream()
    data_stream.print_processors_stats()
    print()

    print("Registering Processors")
    print()
    num_processor = NumericProcessor()
    txt_processor = TextProcessor()
    log_processor = LogProcessor()

    data_stream.register_processor(num_processor)
    data_stream.register_processor(txt_processor)
    data_stream.register_processor(log_processor)

    log_1 = [
        {"log_level": "WARNING",
         "log_message": "Telnet access! Use ssh instead"},
        {"log_level": "INFO",
         "log_message": "User wil is connected"},
    ]
    stream_1 = [
        "Hello world",
        [3.14, -1, 2.71],
        log_1,
        42,
        ["Hi", "five"],
    ]
    print(f"Send first batch of data on stream: {stream_1}")
    data_stream.process_stream(stream_1)
    data_stream.print_processors_stats()
    print()

    print("Send 3 processed data from each processor to a CSV plugin:")
    csv_plugin = CSVExportPlugin()
    data_stream.output_pipeline(3, csv_plugin)
    data_stream.print_processors_stats()
    print()

    log_2 = [
        {"log_level": "ERROR",
         "log_message": "500 server crash"},
        {"log_level": "NOTICE",
         "log_message": "Certificate expires in 10 days"},
    ]
    stream_2 = [
        21,
        ["I love AI", "LLMs are wonderful", "Stay healthy"],
        log_2,
        [32, 42, 64, 84, 128, 168],
        "World hello",
    ]
    print(f"Send another batch of data: {stream_2}")
    data_stream.process_stream(stream_2)
    data_stream.print_processors_stats()
    print()

    print("Send 5 processed data from each processor to a JSON plugin:")
    json_plugin = JSONExportPlugin()
    data_stream.output_pipeline(5, json_plugin)
    print()

    data_stream.print_processors_stats()


if __name__ == "__main__":
    data_pipeline()
